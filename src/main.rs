use axum::{
    extract::multipart::MultipartError,
    extract::Multipart,
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::post,
    Json, Router,
};
use chrono::Utc;
use clap::Parser;
use interprocess::unnamed_pipe::tokio::{pipe, Sender};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::Cursor,
    num::ParseIntError,
    os::unix::io::AsRawFd,
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{mpsc, Mutex},
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
}

#[derive(Error, Debug)]
enum ApiError {
    #[error("Serialization failed: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),
    #[error("Deserialization failed: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Channel closed")]
    ChannelClosed,
    #[error("JSON serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Invalid size format: {0}")]
    InvalidSizeFormat(String),
    #[error("Missing Field: {0}")]
    MissingField(String),
    #[error("Multipart error: {0}")]
    Multipart(#[from] MultipartError),
    #[error("Invalid integer field: {0}")]
    InvalidIntField(#[from] ParseIntError),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, openai_error_type, param, message) = match &self {
            ApiError::InvalidSizeFormat(val) => (
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                Some("size"),
                format!("Invalid value for 'size'. Expected format like '1024x1024', got: '{}'", val),
            ),
            ApiError::MissingField(field) => (
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                Some(field.as_str()),
                format!("Missing required parameter: '{}'", field),
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
             ApiError::Serialization(_) |
             ApiError::Deserialization(_) | // Error deserializing worker response is an internal issue
             ApiError::Io(_) |
             ApiError::ChannelClosed | // Internal communication failure
             ApiError::Json(_) => {
                 error!(error.message = %self, error.type = "api_error", "Internal Server Error occurred");
                 (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "api_error",
                    None,
                    "An unexpected internal server error occurred.".to_string(),
                 )
             }
        };
        if status.is_client_error() {
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

#[derive(Serialize, Deserialize)]
struct GenerateRequest {
    prompt: String,
    n: Option<u32>,
    size: Option<String>,
    response_format: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct GenerateResponse {
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
struct ImageEditRequest {
    prompt: String,
    n: Option<u32>,
    size: Option<String>,
    response_format: Option<String>,
    image: String,
    mask: Option<String>,
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

#[derive(Serialize, Deserialize)]
struct MessageRequest<T> {
    id: u64,
    name: String,
    data: T,
}

#[derive(Serialize, Deserialize)]
struct Message<T> {
    id: u64,
    data: T,
}

struct WorkerResponse {
    id: u64,
    rx: mpsc::Receiver<Vec<u8>>,
    subs: Arc<Mutex<HashMap<u64, mpsc::Sender<Vec<u8>>>>>,
}

impl WorkerResponse {
    async fn next<T: DeserializeOwned>(&mut self) -> Result<T, ApiError> {
        let raw = self.rx.recv().await.ok_or(ApiError::ChannelClosed)?;
        let msg: Message<T> = rmp_serde::from_slice(&raw)?;
        Ok(msg.data)
    }
}

impl Drop for WorkerResponse {
    fn drop(&mut self) {
        let subs = self.subs.clone();
        let id = self.id;
        tokio::spawn(async move {
            if subs.lock().await.remove(&id).is_some() {
                info!(request_id = id, "Subscription removed.");
            };
        });
    }
}

struct AppState {
    writer: Mutex<Sender>,
    subs: Arc<Mutex<HashMap<u64, mpsc::Sender<Vec<u8>>>>>,
    id_counter: AtomicU64,
}

impl AppState {
    async fn call<T: Serialize>(
        &self,
        name: &str,
        data: T,
        buffer_size: usize,
    ) -> Result<WorkerResponse, ApiError> {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(buffer_size);
        self.subs.lock().await.insert(id, tx);

        let message = MessageRequest {
            id,
            name: name.into(),
            data,
        };
        let raw = rmp_serde::to_vec_named(&message)?;
        info!(
            request_id = id,
            handler_name = name,
            msg_size = raw.len(),
            "Sending request to worker"
        );
        self.writer.lock().await.write_all(&raw).await?;

        Ok(WorkerResponse {
            id,
            rx,
            subs: self.subs.clone(),
        })
    }
}

async fn images_generations(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<GenerateRequest>,
) -> Result<Json<GenerateResponse>, ApiError> {
    let n = payload.n.unwrap_or(1);
    let size_str = payload.size.unwrap_or("1024x1024".into());
    let response_format = payload.response_format.unwrap_or("b64_json".into());

    let (width, height) = size_str
        .split_once('x')
        .and_then(|(w, h)| Some((w.trim().parse().ok()?, h.trim().parse().ok()?)))
        .ok_or(ApiError::InvalidSizeFormat(size_str.clone()))?;

    #[derive(Serialize)]
    struct Request {
        prompt: String,
        n: u32,
        width: u32,
        height: u32,
        response_format: String,
    }
    let request = Request {
        prompt: payload.prompt,
        n,
        width,
        height,
        response_format,
    };
    let mut worker = state.call("images_generations", request, n as _).await?;

    let mut data = Vec::with_capacity(n as usize);
    for _ in 0..n {
        data.push(worker.next::<ImageData>().await?);
    }

    Ok(Json(GenerateResponse {
        created: Utc::now().timestamp(),
        data,
    }))
}

async fn images_editions(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<Json<GenerateResponse>, ApiError> {
    let mut prompt = None;
    let mut n = None;
    let mut size = None;
    let mut response_format = None;
    let mut image = None;
    let mut mask = None;

    // Extract fields from multipart data
    while let Some(field) = multipart.next_field().await? {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "prompt" => prompt = Some(field.text().await?),
            "n" => n = Some(field.text().await?.parse::<u32>()?),
            "size" => size = Some(field.text().await?),
            "response_format" => response_format = Some(field.text().await?),
            "image" => image = Some(field.bytes().await?.to_vec()),
            "mask" => mask = Some(field.bytes().await?.to_vec()),
            _ => {}
        }
    }
    let prompt = prompt.ok_or(ApiError::MissingField("prompt".into()))?;
    let n = n.unwrap_or(1);
    let size_str = size.unwrap_or("1024x1024".into());
    let response_format = response_format.unwrap_or("b64_json".into());
    let image = image.ok_or(ApiError::MissingField("image".into()))?;

    let (width, height) = size_str
        .split_once('x')
        .and_then(|(w, h)| Some((w.trim().parse().ok()?, h.trim().parse().ok()?)))
        .ok_or(ApiError::InvalidSizeFormat(size_str.clone()))?;

    #[derive(Serialize)]
    struct EditRequestWrapper {
        prompt: String,
        n: u32,
        width: u32,
        height: u32,
        response_format: String,
        #[serde(with = "serde_bytes")]
        image: Vec<u8>,
        #[serde(with = "serde_bytes", skip_serializing_if = "Option::is_none")] // Crucial
        mask: Option<Vec<u8>>,
    }
    let request = EditRequestWrapper {
        prompt,
        n,
        width,
        height,
        response_format,
        image,
        mask,
    };
    let mut worker = state.call("images_editions", request, n as _).await?;

    let mut data = Vec::with_capacity(n as usize);
    for _ in 0..n {
        data.push(worker.next::<ImageData>().await?);
    }
    Ok(Json(GenerateResponse {
        created: Utc::now().timestamp(),
        data,
    }))
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
    let (write_request, request_fd) = pipe()?;
    let (reply_fd, read_reply) = pipe()?;

    let request_fd_raw = request_fd.as_raw_fd();
    let reply_fd_raw = reply_fd.as_raw_fd();

    let mut child = Command::new(&args.worker_path)
        .args(&args.worker_args)
        .env("HFENDPOINT_FD_REQUEST", request_fd_raw.to_string())
        .env("HFENDPOINT_FD_REPLY", reply_fd_raw.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;

    drop(request_fd);
    drop(reply_fd);

    let state = Arc::new(AppState {
        writer: Mutex::new(write_request),
        subs: Arc::new(Mutex::new(HashMap::new())),
        id_counter: AtomicU64::new(1),
    });

    tokio::spawn({
        let subs = state.subs.clone();
        async move {
            let mut reader = BufReader::new(read_reply);
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = vec![0; 4096];
            while let Ok(n) = reader.read(&mut chunk).await {
                if n == 0 {
                    info!("Worker reply pipe closed (EOF). Reader task exiting.");
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                let mut cursor = Cursor::new(&buf);
                let mut consumed = 0;
                while let Ok(msg) = rmp_serde::from_read::<_, Message<rmpv::Value>>(&mut cursor) {
                    let pos = cursor.position() as usize;
                    let raw = buf[consumed..pos].to_vec();
                    if let Some(sender) = subs.lock().await.get(&msg.id) {
                        let _ = sender.send(raw).await;
                    } else {
                        warn!(
                            request_id = msg.id,
                            "Subscription not found for received message."
                        );
                    }
                    consumed = pos;
                }
                if consumed > 0 {
                    buf.drain(..consumed);
                }
            }
        }
    });

    let app = Router::new()
        .route("/v1/images/generations", post(images_generations))
        .route("/v1/images/edits", post(images_editions))
        .route("/v1/chat/completions", post(chat_completions))
        .with_state(state);

    let bind = format!("{}:{}", args.host, args.port);
    info!("Binding server to {}", bind);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    axum::serve(listener, app).await?;
    match child.wait().await {
        Ok(status) => println!("Worker exited with status: {}", status),
        Err(e) => eprintln!("Failed waiting for worker: {}", e),
    }
    info!("Shutdown complete.");
    Ok(())
}
