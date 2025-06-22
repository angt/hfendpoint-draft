use axum::{
    body::Bytes,
    extract::multipart::MultipartError,
    extract::DefaultBodyLimit,
    extract::Path,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use axum_typed_multipart::{TryFromMultipart, TypedMultipart};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::BytesMut;
use chrono::Utc;
use clap::Parser;
use dashmap::DashMap;
use nix::fcntl::{fcntl, FcntlArg, FdFlag};
use nix::sys::socket::{setsockopt, sockopt};
use rmp_serde::decode::Error as RmpDecodeError;
use rmp_serde::encode::Error as RmpEncodeError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    convert::TryFrom,
    io::{Cursor, ErrorKind},
    num::ParseIntError,
    os::unix::io::AsRawFd,
    process::Stdio,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{unix::OwnedWriteHalf, UnixStream},
    process::Command,
    signal::unix::{signal, SignalKind},
    sync::{mpsc, oneshot, Mutex},
};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[clap(version, about, long_about = None)]
struct Args {
    /// Host address to bind to
    #[clap(long, default_value = "0.0.0.0")]
    host: String,

    /// Port to listen on
    #[clap(long, default_value = "3000")]
    port: u16,

    /// Path to the worker executable
    worker_path: String,

    /// Arguments to pass to the worker executable
    worker_args: Vec<String>,

    /// Maximum memory capacity for images in bytes
    #[clap(long, default_value = "1G", value_parser = parse_size)]
    max_image_capacity: usize,

    /// Maximum request body size
    #[clap(long, default_value = "10M", value_parser = parse_size)]
    max_body_size: usize,
}

fn parse_size(input: &str) -> Result<usize, String> {
    let input = input.trim();
    let (value, suffix) = input.chars().partition::<String, _>(|c| c.is_ascii_digit());

    let multiplier: usize = match suffix.to_ascii_uppercase().as_str() {
        "" => 1,
        "K" => 1024,
        "M" => 1024 * 1024,
        "G" => 1024 * 1024 * 1024,
        _ => return Err(format!("Invalid suffix: '{}'", suffix)),
    };
    let number = value
        .parse::<usize>()
        .map_err(|e| format!("Invalid number: {}", e))?;

    if number == 0 {
        return Err("Value must be strictly positive".into());
    }
    Ok(number * multiplier)
}

#[derive(Serialize)]
struct ApiErrorDetail {
    message: String,
    r#type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    param: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

#[derive(Serialize)]
struct ApiErrorResponse {
    error: ApiErrorDetail,
}

#[derive(Error, Debug)]
enum ApiError {
    #[error("Rmp encode error: {0}")]
    RmpEncode(#[from] RmpEncodeError),
    #[error("Rmp decode error: {0}")]
    RmpDecode(#[from] RmpDecodeError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Channel closed")]
    ChannelClosed,
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Multipart error: {0}")]
    Multipart(#[from] MultipartError),
    #[error("Invalid parameter value for '{param}': {msg}")]
    InvalidParameterValue { param: &'static str, msg: String },
    #[error("Worker cannot accept new requests")]
    ServiceUnavailable,
    #[error("Worker process failed: {0}")]
    InternalServerError(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, r#type, param, message) = match &self {
            ApiError::Multipart(e) => (
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                None,
                format!("Invalid multipart/form-data request: {}", e),
            ),
            ApiError::InvalidParameterValue { param, msg } => (
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                Some(*param),
                msg.clone(),
            ),
            ApiError::ServiceUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "api_error",
                None,
                "The engine is currently overloaded, please try again later".to_string(),
            ),
            ApiError::InternalServerError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_error",
                None,
                msg.to_string(),
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_error",
                None,
                "The server had an error while processing your request".to_string(),
            ),
        };
        if status.is_server_error() {
            error!(
                status = %status,
                r#type = r#type,
                param = ?param,
                message = message,
                %self,
            );
        } else if status.is_client_error() {
            info!(
                status = %status,
                r#type = r#type,
                param = ?param,
                message = message,
                %self,
            );
        }
        let response = ApiErrorResponse {
            error: ApiErrorDetail {
                message,
                r#type,
                param,
                code: None,
            },
        };
        (status, Json(response)).into_response()
    }
}

fn base_url(headers: &HeaderMap) -> String {
    let scheme = headers
        .get("X-Forwarded-Proto")
        .and_then(|value| value.to_str().ok())
        .and_then(|protos| protos.split(',').next())
        .map(|proto| proto.trim())
        .filter(|proto| !proto.is_empty())
        .unwrap_or("http");

    let authority = headers
        .get("X-Forwarded-Host")
        .or_else(|| headers.get(header::HOST))
        .and_then(|value| value.to_str().ok())
        .and_then(|hosts| hosts.split(',').next())
        .map(|host| host.trim())
        .filter(|host| !host.is_empty());

    if let Some(auth) = authority {
        let base = format!("{}://{}", scheme, auth);
        return base.trim_end_matches('/').to_string();
    }
    warn!("Could not determine host from headers.");
    String::new()
}

#[derive(Deserialize)]
struct ImagesGenerationsRequest {
    prompt: String,
    n: Option<u32>,
    size: Option<String>,
    response_format: Option<String>,
}

#[derive(Serialize)]
struct ImagesResponse {
    created: i64,
    data: Vec<ImageData>,
}

#[derive(Serialize, Deserialize)]
struct ImageData {
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    b64_json: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct ChatRequest {
    messages: Vec<ChatMessage>,
    stream: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone)]
struct ChatMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct ChatChoiceMessage {
    index: u32,
    message: ChatMessage,
    finish_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    logprobs: Option<()>,
}

#[derive(Serialize, Deserialize)]
struct ChatChoiceDelta {
    index: u32,
    delta: ChatMessage,
    #[serde(skip_serializing_if = "Option::is_none")]
    finish_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    logprobs: Option<()>,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum ChatChoice {
    Delta(ChatChoiceDelta),
    Message(ChatChoiceMessage),
}

#[derive(Serialize, Deserialize)]
struct ChatResponse {
    id: String,
    object: String,
    created: i64,
    model: Option<String>,
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
enum EmbeddingInput {
    String(String),
    StringArray(Vec<String>),
    Tokens(Vec<u32>),
    TokenArray(Vec<Vec<u32>>),
}

#[derive(Deserialize, Debug)]
struct EmbeddingRequest {
    input: EmbeddingInput,
    encoding_format: Option<String>,
}

#[derive(Serialize, Debug)]
struct EmbeddingObject {
    object: String,
    embedding: Vec<f32>,
    index: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct UsageStats {
    prompt_tokens: u32,
    total_tokens: u32,
}

#[derive(Serialize, Debug)]
struct EmbeddingResponse {
    object: String,
    data: Vec<EmbeddingObject>,
    model: String,
    usage: UsageStats,
}

#[derive(Serialize, Deserialize)]
struct WorkerMessage<T> {
    id: u64,
    data: Option<T>,
    error: Option<String>,
}

struct Worker {
    id: u64,
    rx: mpsc::Receiver<Bytes>,
    subs: Arc<DashMap<u64, mpsc::Sender<Bytes>>>,
    start_time: Instant,
}

impl Worker {
    async fn next<T: DeserializeOwned>(&mut self) -> Result<T, ApiError> {
        const TIMEOUT: Duration = Duration::from_secs(5 * 60);

        let raw = tokio::time::timeout(TIMEOUT, self.rx.recv())
            .await
            .map_err(|_| ApiError::InternalServerError("Worker response timed out".into()))?
            .ok_or(ApiError::ChannelClosed)?;

        let msg: WorkerMessage<T> = rmp_serde::from_slice(&raw)?;

        match msg.data {
            Some(data) => Ok(data),
            None => {
                error!(
                    request_id = msg.id,
                    "Worker sent null data for type {}",
                    std::any::type_name::<T>()
                );
                let err_msg = msg.error.unwrap_or_else(|| {
                    "The worker process encountered an unspecified error".to_string()
                });
                Err(ApiError::InternalServerError(err_msg))
            }
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(_) = self.subs.remove(&self.id) {
            let duration = self.start_time.elapsed();
            info!(
                request_id = self.id,
                duration_ms = duration.as_millis(),
                "Subscription removed."
            );
        }
    }
}

struct ImageStore {
    data: DashMap<String, Bytes>,
    queue: Mutex<VecDeque<String>>,
    current_size: AtomicUsize,
    max_capacity: usize,
}

impl ImageStore {
    fn new(max_capacity: usize) -> Self {
        Self {
            data: DashMap::new(),
            queue: Mutex::new(VecDeque::new()),
            current_size: AtomicUsize::new(0),
            max_capacity,
        }
    }

    async fn insert(&self, id: String, image: Bytes) {
        let size = image.len();

        self.data.insert(id.clone(), image);
        self.current_size.fetch_add(size, Ordering::Relaxed);

        let mut queue = self.queue.lock().await;
        queue.push_back(id);

        while self.current_size.load(Ordering::Relaxed) > self.max_capacity {
            if let Some(old_id) = queue.pop_front() {
                if let Some((_, old_img)) = self.data.remove(&old_id) {
                    self.current_size
                        .fetch_sub(old_img.len(), Ordering::Relaxed);
                }
            } else {
                break;
            }
        }
    }

    async fn delete(&self, id: &str) -> bool {
        let mut found = false;

        if let Some((_, bytes)) = self.data.remove(id) {
            self.current_size.fetch_sub(bytes.len(), Ordering::Relaxed);
            found = true;
        }
        let mut queue = self.queue.lock().await;

        if let Some(pos) = queue.iter().position(|x| x == id) {
            queue.remove(pos);
        }
        found
    }
}

struct AppState {
    writer: Mutex<Option<OwnedWriteHalf>>,
    subs: Arc<DashMap<u64, mpsc::Sender<Bytes>>>,
    id_counter: AtomicU64,
    image_store: ImageStore,
}

impl AppState {
    async fn call<T: Serialize>(
        &self,
        name: &str,
        data: T,
        buffer_size: usize,
    ) -> Result<Worker, ApiError> {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);

        #[derive(Serialize)]
        struct WorkerRequest<T> {
            id: u64,
            name: String,
            data: T,
        }
        let request = WorkerRequest {
            id,
            name: name.into(),
            data,
        };
        let raw = rmp_serde::to_vec_named(&request)?;

        let mut writer_guard = self.writer.lock().await;
        let sender = writer_guard.as_mut().ok_or(ApiError::ServiceUnavailable)?;

        let (tx, rx) = mpsc::channel::<Bytes>(buffer_size);
        self.subs.insert(id, tx);

        let start_time = Instant::now();
        let sender_return = sender.write_all(&raw).await;
        drop(writer_guard);

        match sender_return {
            Ok(_) => {
                info!(request_id = id, handler_name = name, "Request worker");
                Ok(Worker {
                    id,
                    rx,
                    subs: self.subs.clone(),
                    start_time,
                })
            }
            Err(e) => {
                self.subs.remove(&id);
                error!(
                    request_id = id,
                    handler_name = name,
                    "IPC write to worker failed: {}",
                    e
                );
                Err(ApiError::ServiceUnavailable)
            }
        }
    }
}

fn parse_wxh(size: &str) -> Result<(u32, u32), ApiError> {
    size.split_once('x')
        .and_then(|(w, h)| Some((w.trim().parse().ok()?, h.trim().parse().ok()?)))
        .ok_or(ApiError::InvalidParameterValue {
            param: "size",
            msg: size.to_string(),
        })
}

#[derive(Deserialize)]
struct Png {
    png: Bytes,
}

#[derive(Copy, Clone)]
enum ImageResponseFormat {
    Url,
    B64Json,
}

impl TryFrom<Option<String>> for ImageResponseFormat {
    type Error = ApiError;

    fn try_from(opt: Option<String>) -> Result<Self, Self::Error> {
        let format = opt.unwrap_or_else(|| "b64_json".to_string());
        match format.as_str() {
            "url" => Ok(ImageResponseFormat::Url),
            "b64_json" => Ok(ImageResponseFormat::B64Json),
            _ => Err(ApiError::InvalidParameterValue {
                param: "response_format",
                msg: format!("Response format {} not supported", format),
            }),
        }
    }
}

async fn process_image_response(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    worker_id: u64,
    image_id: u32,
    image_bytes: Bytes,
    format: ImageResponseFormat,
) -> ImageData {
    match format {
        ImageResponseFormat::B64Json => ImageData {
            b64_json: Some(STANDARD.encode(&image_bytes)),
            url: None,
        },
        ImageResponseFormat::Url => {
            let id = format!("{}-{}", worker_id, image_id);
            let url = format!("{}/v1/images/{}", base_url(headers), id);
            state.image_store.insert(id, image_bytes).await;
            ImageData {
                url: Some(url),
                b64_json: None,
            }
        }
    }
}

async fn images_generations(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<ImagesGenerationsRequest>,
) -> Result<Json<ImagesResponse>, ApiError> {
    let n = payload.n.unwrap_or(1).clamp(1, 4);
    let response_format = ImageResponseFormat::try_from(payload.response_format)?;
    let (width, height) = parse_wxh(&payload.size.unwrap_or("1024x1024".into()))?;

    #[derive(Serialize)]
    struct WorkerRequest {
        prompt: String,
        n: u32,
        width: u32,
        height: u32,
    }
    let request = WorkerRequest {
        prompt: payload.prompt,
        n,
        width,
        height,
    };
    let mut worker = state.call("images_generations", request, n as _).await?;
    let mut data = Vec::with_capacity(n as usize);

    for image_id in 0..n {
        let image = worker.next::<Png>().await?;
        let image_bytes = Bytes::from(image.png);
        data.push(
            process_image_response(
                &state,
                &headers,
                worker.id,
                image_id,
                image_bytes,
                response_format,
            )
            .await,
        );
    }

    Ok(Json(ImagesResponse {
        created: Utc::now().timestamp(),
        data,
    }))
}

#[derive(TryFromMultipart)]
struct ImageEditionsRequestData {
    prompt: String,
    n: Option<String>,
    size: Option<String>,
    response_format: Option<String>,
    image: Bytes,
    mask: Option<Bytes>,
}

async fn images_editions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    TypedMultipart(payload): TypedMultipart<ImageEditionsRequestData>,
) -> Result<Json<ImagesResponse>, ApiError> {
    let n = payload
        .n
        .map(|s| s.parse::<u32>())
        .transpose()
        .map_err(|e: ParseIntError| ApiError::InvalidParameterValue {
            param: "n",
            msg: format!("Failed to parse integer: {}", e),
        })?
        .unwrap_or(1);
    let response_format = ImageResponseFormat::try_from(payload.response_format)?;
    let (width, height) = parse_wxh(&payload.size.unwrap_or("1024x1024".into()))?;

    #[derive(Serialize)]
    struct WorkerRequest {
        prompt: String,
        n: u32,
        width: u32,
        height: u32,
        image: Bytes,
        #[serde(skip_serializing_if = "Option::is_none")]
        mask: Option<Bytes>,
    }
    let request = WorkerRequest {
        prompt: payload.prompt,
        n,
        width,
        height,
        image: payload.image,
        mask: payload.mask,
    };
    let mut worker = state.call("images_editions", request, n as _).await?;
    let mut data = Vec::with_capacity(n as usize);

    for image_id in 0..n {
        let image = worker.next::<Png>().await?;
        let image_bytes = Bytes::from(image.png);
        data.push(
            process_image_response(
                &state,
                &headers,
                worker.id,
                image_id,
                image_bytes,
                response_format,
            )
            .await,
        );
    }
    Ok(Json(ImagesResponse {
        created: Utc::now().timestamp(),
        data,
    }))
}

async fn delete_images(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if state.image_store.delete(&id).await {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn get_images(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Some(image) = state.image_store.data.get(&id) {
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "image/png")
            .body(image.value().clone().into())
            .unwrap()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn chat_completions(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ChatRequest>,
) -> Result<Response, ApiError> {
    let is_stream = payload.stream.unwrap_or(false);
    let mut worker = state.call("chat_completions", payload, 10).await?;

    #[derive(Deserialize)]
    struct WorkerResponse {
        content: Option<String>,
        finish_reason: Option<String>,
    }
    if is_stream {
        let mut is_first_chunk = true;
        let stream = async_stream::stream! {
            loop {
                match worker.next::<WorkerResponse>().await {
                    Ok(chunk) => {
                        let is_final = chunk.finish_reason.is_some();
                        let delta = ChatMessage {
                            role: if is_first_chunk {
                                Some("assistant".to_string())
                            } else {
                                None
                            },
                            content: chunk.content,
                        };
                        is_first_chunk = false;
                        let resp = ChatResponse {
                            id: format!("chatcmpl-{}", worker.id),
                            object: "chat.completion.chunk".into(),
                            created: Utc::now().timestamp(),
                            model: None,
                            choices: vec![ChatChoice::Delta(ChatChoiceDelta {
                                index: 0,
                                delta,
                                finish_reason: chunk.finish_reason.clone(),
                                logprobs: None,
                            })],
                        };
                        let json = serde_json::to_string(&resp)?;
                        yield Ok(Event::default().data(json));
                        if is_final {
                            break;
                        }
                    }
                    Err(e) => { yield Err(e); break; }
                }
            }
        };
        Ok(Sse::new(stream).into_response())
    } else {
        let mut message = ChatMessage {
            role: Some("assistant".into()),
            content: Some(String::new()),
        };
        let mut finish_reason = None;
        while let Ok(chunk) = worker.next::<WorkerResponse>().await {
            if let Some(content) = chunk.content {
                message.content.as_mut().unwrap().push_str(&content);
            }
            finish_reason = chunk.finish_reason;
        }
        Ok(Json(ChatResponse {
            id: format!("chatcmpl-{}", worker.id),
            object: "chat.completion".into(),
            created: Utc::now().timestamp(),
            model: None,
            choices: vec![ChatChoice::Message(ChatChoiceMessage {
                index: 0,
                message,
                finish_reason,
                logprobs: None,
            })],
        })
        .into_response())
    }
}

async fn tokenize(state: &Arc<AppState>, input: Vec<String>) -> Result<Vec<Vec<u32>>, ApiError> {
    if input.is_empty() {
        return Err(ApiError::InvalidParameterValue {
            param: "input",
            msg: "Input string array must not be empty".to_string(),
        });
    }
    if let Some(idx) = input.iter().position(|s_item| s_item.is_empty()) {
        return Err(ApiError::InvalidParameterValue {
            param: "input",
            msg: format!("Input string at index {} must not be empty", idx),
        });
    }
    #[derive(Serialize)]
    struct WorkerRequest {
        input: Vec<String>,
    }
    let request = WorkerRequest { input };
    let mut worker = state.call("tokenize", request, 1).await?;

    #[derive(Deserialize, Debug)]
    struct WorkerResponse {
        tokens: Vec<Vec<u32>>,
    }
    let response: WorkerResponse = worker.next().await?;

    Ok(response.tokens)
}

async fn embeddings(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<EmbeddingRequest>,
) -> Result<Json<EmbeddingResponse>, ApiError> {
    let encoding_format = payload.encoding_format.unwrap_or("float".to_string());
    if encoding_format != "float" {
        return Err(ApiError::InvalidParameterValue {
            param: "encoding_format",
            msg: format!("Encoding format {} is not supported", encoding_format),
        });
    }
    let input = match payload.input {
        EmbeddingInput::String(x) => tokenize(&state, vec![x]).await?,
        EmbeddingInput::StringArray(x) => tokenize(&state, x).await?,
        EmbeddingInput::Tokens(x) => vec![x],
        EmbeddingInput::TokenArray(x) => x,
    };
    if input.is_empty() {
        return Err(ApiError::InvalidParameterValue {
            param: "input",
            msg: "Input array must not be empty".to_string(),
        });
    }
    if let Some(idx) = input.iter().position(|v| v.is_empty()) {
        return Err(ApiError::InvalidParameterValue {
            param: "input",
            msg: format!("Input array at index {} must not be empty", idx),
        });
    }
    let total_tokens: u32 = input.iter().map(|v| v.len()).sum::<usize>() as u32;

    #[derive(Serialize)]
    struct WorkerRequest {
        input: Vec<Vec<u32>>,
    }
    let request = WorkerRequest { input };
    let mut worker = state.call("embeddings", request, 1).await?;

    #[derive(Deserialize, Debug)]
    struct WorkerResponse {
        embeddings: Vec<Vec<f32>>,
    }
    let response: WorkerResponse = worker.next().await?;

    let data: Vec<EmbeddingObject> = response
        .embeddings
        .into_iter()
        .enumerate()
        .map(|(index, embedding)| EmbeddingObject {
            object: "embedding".to_string(),
            embedding,
            index,
        })
        .collect();

    let api_response = EmbeddingResponse {
        object: "list".to_string(),
        data,
        model: "unknown".to_string(),
        usage: UsageStats {
            prompt_tokens: total_tokens,
            total_tokens,
        },
    };
    Ok(Json(api_response))
}

fn socketpair(buffer_size: usize) -> Result<(UnixStream, UnixStream), Box<dyn std::error::Error>> {
    let (server, worker) = UnixStream::pair()?;

    setsockopt(&server, sockopt::SndBuf, &buffer_size)?;
    setsockopt(&server, sockopt::RcvBuf, &buffer_size)?;

    setsockopt(&worker, sockopt::SndBuf, &buffer_size)?;
    setsockopt(&worker, sockopt::RcvBuf, &buffer_size)?;

    let mut flags =
        FdFlag::from_bits(fcntl(&worker, FcntlArg::F_GETFD)?).ok_or(nix::errno::Errno::EINVAL)?;

    flags.remove(FdFlag::FD_CLOEXEC);
    fcntl(&worker, FcntlArg::F_SETFD(flags))?;

    Ok((server, worker))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();

    let args = Args::parse();

    const BUFFER_SIZE: usize = 1024 * 1024;
    let (sock, worker_sock) = socketpair(BUFFER_SIZE)?;

    let mut child = Command::new(&args.worker_path)
        .args(&args.worker_args)
        .env("HFENDPOINT_FD", worker_sock.as_raw_fd().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .process_group(0)
        .spawn()?;

    drop(worker_sock);

    let (read_reply, write_request) = sock.into_split();

    let state = Arc::new(AppState {
        writer: Mutex::new(Some(write_request)),
        subs: Arc::new(DashMap::new()),
        id_counter: AtomicU64::new(1),
        image_store: ImageStore::new(args.max_image_capacity),
    });
    let (worker_tx, worker_rx) = oneshot::channel();

    tokio::spawn({
        let subs = state.subs.clone();
        async move {
            let mut reader = BufReader::with_capacity(BUFFER_SIZE, read_reply);
            let mut buf = BytesMut::with_capacity(2 * BUFFER_SIZE);
            while let Ok(n) = reader.read_buf(&mut buf).await {
                if n == 0 {
                    info!("Worker reply pipe closed (EOF). Reader task exiting.");
                    let _ = worker_tx.send(());
                    break;
                }
                while !buf.is_empty() {
                    let mut cursor = Cursor::new(buf.as_ref());
                    match rmp_serde::from_read::<_, WorkerMessage<rmpv::Value>>(&mut cursor) {
                        Ok(msg) => {
                            let pos = cursor.position() as usize;
                            let raw: Bytes = buf.split_to(pos).freeze();
                            if let Some(sender) = subs.get(&msg.id).map(|e| e.value().clone()) {
                                let _ = sender.send(raw).await.map_err(|e| {
                                    error!(request_id = msg.id, "Failed to send message: {:?}", e);
                                });
                            } else {
                                warn!(request_id = msg.id, "Subscription not found");
                            }
                        }
                        Err(RmpDecodeError::InvalidDataRead(ref io_err))
                        | Err(RmpDecodeError::InvalidMarkerRead(ref io_err))
                            if io_err.kind() == ErrorKind::UnexpectedEof =>
                        {
                            break
                        }
                        Err(e) => {
                            error!("Deserialization error: {:?}", e);
                            buf.clear();
                            break;
                        }
                    }
                }
            }
        }
    });

    let stderr_handle = if let Some(stderr) = child.stderr.take() {
        Some(tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                error!("worker stderr: {}", line);
            }
            info!("Worker stderr stream finished.");
        }))
    } else {
        None
    };
    let app = Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/images/generations", post(images_generations))
        .route("/v1/images/edits", post(images_editions))
        .route("/v1/images/{id}", get(get_images).delete(delete_images))
        .route("/v1/embeddings", post(embeddings))
        .layer(DefaultBodyLimit::max(args.max_body_size))
        .with_state(state.clone());

    let bind = format!("{}:{}", args.host, args.port);
    info!("Binding server to {}", bind);

    let listener = tokio::net::TcpListener::bind(&bind).await?;

    let shutdown = {
        let state = state.clone();

        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;

        async move {
            tokio::select! {
                _ = sigint.recv() => info!("Received SIGINT, shutdown."),
                _ = sigterm.recv() => info!("Received SIGTERM, shutdown."),
                _ = worker_rx => info!("Worker exited, shutdown."),
            };
            let mut writer = state.writer.lock().await;
            writer.take();
        }
    };
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;

    if let Some(stderr) = stderr_handle {
        let _ = stderr.await;
    }
    match child.wait().await {
        Ok(status) => println!("Worker exited with status: {}", status),
        Err(e) => eprintln!("Failed waiting for worker: {}", e),
    }
    info!("Shutdown complete.");
    Ok(())
}
