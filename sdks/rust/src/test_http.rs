//! A minimal HTTP/1.1 server for tests, answering one request per connection.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

#[derive(Debug, Clone)]
pub(crate) struct RecordedHttp {
    pub method: String,
    /// Path including the query string.
    pub target: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl RecordedHttp {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    pub fn path(&self) -> &str {
        self.target.split('?').next().unwrap_or(&self.target)
    }

    pub fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

#[derive(Clone, Default)]
pub(crate) struct TestResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub delay: Option<Duration>,
    /// Runs after `delay`, just before the response is written.
    pub after: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl TestResponse {
    pub fn status(status: u16) -> Self {
        Self {
            status,
            ..Default::default()
        }
    }

    pub fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.push((name.to_string(), value.into()));
        self
    }

    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }

    pub fn xml(self, body: &str) -> Self {
        self.header("Content-Type", "application/xml").body(body)
    }

    pub fn json(self, body: serde_json::Value) -> Self {
        self.header("Content-Type", "application/json")
            .body(body.to_string())
    }

    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn after(mut self, hook: impl Fn() + Send + Sync + 'static) -> Self {
        self.after = Some(Arc::new(hook));
        self
    }
}

pub(crate) type Handler = Arc<dyn Fn(&RecordedHttp) -> TestResponse + Send + Sync>;

pub(crate) struct TestServer {
    pub url: String,
    pub requests: Arc<Mutex<Vec<RecordedHttp>>>,
    task: JoinHandle<()>,
}

impl TestServer {
    pub async fn start(handler: Handler) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let port = listener.local_addr().expect("local addr").port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let handler = handler.clone();
                let recorded = recorded.clone();
                tokio::spawn(async move {
                    let Some(request) = read_request(&mut socket).await else {
                        return;
                    };
                    recorded.lock().unwrap().push(request.clone());
                    let response = handler(&request);
                    if let Some(delay) = response.delay {
                        tokio::time::sleep(delay).await;
                    }
                    if let Some(after) = &response.after {
                        after();
                    }
                    let mut head = format!("HTTP/1.1 {} Test\r\n", response.status);
                    let mut has_length = false;
                    for (name, value) in &response.headers {
                        if name.eq_ignore_ascii_case("content-length") {
                            has_length = true;
                        }
                        head.push_str(&format!("{name}: {value}\r\n"));
                    }
                    if !has_length {
                        head.push_str(&format!("Content-Length: {}\r\n", response.body.len()));
                    }
                    head.push_str("Connection: close\r\n\r\n");
                    let _ = socket.write_all(head.as_bytes()).await;
                    if request.method != "HEAD" {
                        let _ = socket.write_all(&response.body).await;
                    }
                    let _ = socket.shutdown().await;
                });
            }
        });
        Self {
            url: format!("http://127.0.0.1:{port}"),
            requests,
            task,
        }
    }

    pub fn requests(&self) -> Vec<RecordedHttp> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<RecordedHttp> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break position;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next()?.split(' ');
    let method = request_line.next()?.to_string();
    let target = request_line.next()?.to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = buffer[header_end + 4..].to_vec();
    while body.len() < length {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    Some(RecordedHttp {
        method,
        target,
        headers,
        body,
    })
}
