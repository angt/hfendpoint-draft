use axum::{
    body::Bytes,
    extract::multipart::MultipartError,
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
    time::Instant,
};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{unix::OwnedWriteHalf, UnixStream},
    process::Command,
    signal::unix::{signal, SignalKind},
    sync::{mpsc, oneshot, Mutex},
};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

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
    #[clap(long, default_value = "1073741824")]
    max_image_capacity: usize,
}

#[derive(Error, Debug)]
enum ApiError {
    #[error("Serialization failed: {0}")]
    Serialization(#[from] RmpEncodeError),
    #[error("Deserialization failed: {0}")]
    Deserialization(#[from] RmpDecodeError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Channel closed")]
    ChannelClosed,
    #[error("JSON serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Invalid size format: {0}")]
    InvalidSizeFormat(String),
    #[error("Multipart error: {0}")]
    Multipart(#[from] MultipartError),
    #[error("Invalid integer field: {0}")]
    InvalidIntField(#[from] ParseIntError),
    #[error("Invalid value '{value}' for parameter '{param}'")]
    InvalidParameterValue { param: String, value: String },
    #[error("Worker cannot accept new requests.")]
    WorkerUnavailable,
    #[error("Failed to install signal handler: {0}")]
    SignalSetup(std::io::Error),
    #[error("Worker process failed: {0}")]
    WorkerError(String),
    #[error("Internal server error: {0}")]
    InternalServerError(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, openai_error_type, param, message) = match &self {
            ApiError::InvalidSizeFormat(val) => (
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                Some("size"),
                format!(
                    "Invalid value for 'size'. Expected format like '1024x1024', got: '{}'",
                    val
                ),
            ),
            ApiError::Multipart(e) => (
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                None,
                format!("Invalid multipart/form-data request: {}", e),
            ),
            ApiError::InvalidIntField(e) => (
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                None,
                format!("Invalid integer value provided in a request field: {}", e),
            ),
            ApiError::InvalidParameterValue { param, value } => (
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                Some(param.as_str()),
                format!("Invalid value '{value}' for parameter '{param}'"),
            ),
            ApiError::WorkerUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "api_error",
                None,
                "Please try again later.".to_string(),
            ),
            ApiError::WorkerError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_error",
                None,
                format!("Worker error: {}", msg),
            ),
            ApiError::InternalServerError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_error",
                None,
                msg.clone(),
            ),
            _ => {
                error!(error.message = %self, error.type = "api_error", "Internal Server Error occurred");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "api_error",
                    None,
                    "An unexpected internal server error occurred.".to_string(),
                )
            }
        };
        if status.is_server_error() {
            error!(error.message = %self, error.type = openai_error_type, param = ?param, "Server/Worker error occurred");
        } else if status.is_client_error() {
            info!(error.message = %self, error.type = openai_error_type, param = ?param, "Client error occurred");
        }
        let mut error_payload = serde_json::json!({
            "message": message,
            "type": openai_error_type,
            "param": serde_json::Value::Null,
            "code": serde_json::Value::Null,
        });
        if let Some(p) = param {
            error_payload.as_object_mut().unwrap().insert(
                "param".to_string(),
                serde_json::Value::String(p.to_string()),
            );
        }
        let body = Json(serde_json::json!({ "error": error_payload }));
        (status, body).into_response()
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
}

#[derive(Deserialize, Debug)]
struct EmbeddingRequest {
    input: EmbeddingInput,
    model: String,
    encoding_format: Option<String>,
    dimensions: Option<u32>,
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
        let raw = self.rx.recv().await.ok_or(ApiError::ChannelClosed)?;
        let msg: WorkerMessage<T> = rmp_serde::from_slice(&raw).map_err(|e| {
            error!("Failed to deserialize worker message: {}", e);
            ApiError::Deserialization(e)
        })?;
        if let Some(err_msg) = msg.error {
            warn!(request_id = msg.id, "Worker reported error: {}", err_msg);
            return Err(ApiError::WorkerError(err_msg));
        }
        match msg.data {
            Some(data) => Ok(data),
            None => {
                error!(
                    request_id = msg.id,
                    "Worker sent null data without an explicit error for type {}",
                    std::any::type_name::<T>()
                );
                Err(ApiError::InternalServerError(
                    "Worker returned no data and no error".to_string(),
                ))
            }
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let subs = self.subs.clone();
        let id = self.id;
        let start_time = self.start_time;
        tokio::spawn(async move {
            if subs.remove(&id).is_some() {
                let duration = start_time.elapsed();
                info!(
                    request_id = id,
                    duration_ms = duration.as_millis(),
                    "Subscription removed."
                );
            };
        });
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
                    self.current_size.fetch_sub(old_img.len(), Ordering::Relaxed);
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
        let start_time = Instant::now();
        {
            let mut writer_guard = self.writer.lock().await;
            let sender = writer_guard.as_mut().ok_or(ApiError::WorkerUnavailable)?;
            sender.write_all(&raw).await?;
        }
        info!(request_id = id, handler_name = name, "Request worker");

        let (tx, rx) = mpsc::channel::<Bytes>(buffer_size);
        self.subs.insert(id, tx);

        Ok(Worker {
            id,
            rx,
            subs: self.subs.clone(),
            start_time,
        })
    }
}

fn parse_size(size: &str) -> Result<(u32, u32), ApiError> {
    size.split_once('x')
        .and_then(|(w, h)| Some((w.trim().parse().ok()?, h.trim().parse().ok()?)))
        .ok_or(ApiError::InvalidSizeFormat(size.to_string()))
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
            bad => Err(ApiError::InvalidParameterValue {
                param: "response_format".to_string(),
                value: bad.to_string(),
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
    let n = payload.n.unwrap_or(1);
    let response_format = ImageResponseFormat::try_from(payload.response_format)?;
    let (width, height) = parse_size(&payload.size.unwrap_or("1024x1024".into()))?;

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
        .transpose()?
        .unwrap_or(1);
    let response_format = ImageResponseFormat::try_from(payload.response_format)?;
    let (width, height) = parse_size(&payload.size.unwrap_or("1024x1024".into()))?;

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

    if is_stream {
        let stream = async_stream::stream! {
            loop {
                let delta: Result<ChatChoiceDelta, _> = worker.next().await;
                match delta {
                    Ok(chunk) => {
                        let is_final = chunk.finish_reason.as_deref() == Some("stop");
                        let resp = ChatResponse {
                            id: format!("chatcmpl-{}", worker.id),
                            object: "chat.completion.chunk".into(),
                            created: Utc::now().timestamp(),
                            model: None,
                            choices: vec![ChatChoice::Delta(chunk)],
                        };
                        let json = serde_json::to_string(&resp).map_err(ApiError::Json)?;
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

        while let Ok(delta) = worker.next::<ChatChoiceDelta>().await {
            if let Some(content) = delta.delta.content {
                message.content.as_mut().unwrap().push_str(&content);
            }
            finish_reason = delta.finish_reason;
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

async fn embeddings(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<EmbeddingRequest>,
) -> Result<Json<EmbeddingResponse>, ApiError> {
    let encoding_format = payload.encoding_format.unwrap_or("float".to_string());
    if encoding_format != "float" {
        return Err(ApiError::InvalidParameterValue {
            param: "encoding_format".to_string(),
            value: encoding_format,
        });
    }
    let input = match payload.input {
        EmbeddingInput::String(s) => vec![s],
        EmbeddingInput::StringArray(arr) => arr,
    };

    #[derive(Serialize)]
    struct WorkerRequest {
        input: Vec<String>,
        model: String,
        dimensions: Option<u32>,
    }
    let request = WorkerRequest {
        input,
        model: payload.model.clone(),
        dimensions: payload.dimensions,
    };
    let mut worker = state.call("embeddings", request, 1).await?;

    #[derive(Deserialize, Debug)]
    struct WorkerResponse {
        embeddings: Vec<Vec<f32>>,
        usage: UsageStats,
        model: String,
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
        model: response.model,
        usage: response.usage,
    };
    Ok(Json(api_response))
}

fn raw_fd_no_cloexec(stream: &UnixStream) -> nix::Result<i32> {
    let fd = stream.as_raw_fd();
    let flags = FdFlag::from_bits_truncate(fcntl(fd, FcntlArg::F_GETFD)?);
    let new_flags = flags.difference(FdFlag::FD_CLOEXEC);
    fcntl(fd, FcntlArg::F_SETFD(new_flags))?;
    Ok(fd)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Setting default tracing subscriber failed");

    let args = Args::parse();

    let (sock, worker_sock) = UnixStream::pair()?;
    let worker_fd = raw_fd_no_cloexec(&worker_sock)?;

    let mut child = Command::new(&args.worker_path)
        .args(&args.worker_args)
        .env("HFENDPOINT_FD", worker_fd.to_string())
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
            let mut reader = BufReader::with_capacity(8192, read_reply);
            let mut buf = BytesMut::with_capacity(2 * 8192);
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
                            if let Some(sender) = subs.get(&msg.id) {
                                let _ = sender.send(raw).await;
                            } else {
                                warn!(
                                    request_id = msg.id,
                                    "Subscription not found for received message."
                                );
                            }
                        }
                        Err(RmpDecodeError::InvalidDataRead(ref io_err))
                        | Err(RmpDecodeError::InvalidMarkerRead(ref io_err))
                            if io_err.kind() == ErrorKind::UnexpectedEof =>
                        {
                            break
                        }
                        Err(e) => {
                            error!("Deserialization error: {:?}. Clearing buffer.", e);
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
        .route("/v1/images/{image_id}", get(get_images).delete(delete_images))
        .route("/v1/embeddings", post(embeddings))
        .with_state(state.clone());

    let bind = format!("{}:{}", args.host, args.port);
    info!("Binding server to {}", bind);

    let listener = tokio::net::TcpListener::bind(&bind).await?;

    let shutdown = {
        let state = state.clone();

        let mut sigint = signal(SignalKind::interrupt()).map_err(ApiError::SignalSetup)?;
        let mut sigterm = signal(SignalKind::terminate()).map_err(ApiError::SignalSetup)?;

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
