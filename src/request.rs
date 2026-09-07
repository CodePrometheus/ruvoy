use std::time::{Duration, Instant};

/// One request, owned outright so it can cross into the Ruby runtime thread.
#[derive(Clone, Debug)]
pub struct Request {
    /// HTTP method.
    pub method: String,
    /// Request target, query string included.
    pub path: String,
    /// Body bytes already collected. Empty when `body_stream` carries them.
    pub body: Vec<u8>,
    /// A body still arriving, delivered chunk by chunk instead of collected.
    pub body_stream: Option<crate::StreamHandle>,
    /// Request headers in arrival order, values kept as raw bytes.
    pub headers: Vec<(String, Vec<u8>)>,
    /// Connection facts the Rack environment needs.
    pub metadata: RequestMetadata,
    /// Runs a collection before the application is called.
    pub force_gc: bool,
    /// Stage timings, collected only when diagnostics are enabled.
    pub diagnostics: Option<RequestDiagnostics>,
}

impl Request {
    /// Builds a request with default metadata and no headers.
    pub fn new(
        method: impl Into<String>,
        path: impl Into<String>,
        body: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            method: method.into(),
            path: path.into(),
            body: body.into(),
            body_stream: None,
            headers: Vec::new(),
            metadata: RequestMetadata::default(),
            force_gc: false,
            diagnostics: None,
        }
    }

    /// Appends one header.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Requests a collection before the application runs.
    #[must_use]
    pub fn with_forced_gc(mut self) -> Self {
        self.force_gc = true;
        self
    }

    /// Replaces the connection metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: RequestMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Connection facts that the Rack environment exposes to the application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestMetadata {
    /// Value of the `Host` or `:authority` header.
    pub authority: String,
    /// URL scheme, `http` or `https`.
    pub scheme: String,
    /// Host part of the authority, without the port.
    pub server_name: String,
    /// Port the connection was accepted on.
    pub server_port: u16,
    /// Protocol version, for example `HTTP/1.1`.
    pub protocol: String,
    /// Peer address, when Envoy reports one.
    pub remote_addr: Option<String>,
}

impl Default for RequestMetadata {
    fn default() -> Self {
        Self {
            authority: "localhost".to_owned(),
            scheme: "http".to_owned(),
            server_name: "localhost".to_owned(),
            server_port: 80,
            protocol: "HTTP/1.1".to_owned(),
            remote_addr: None,
        }
    }
}

/// Per-stage timings reported back as `x-ruvoy-stage-*` response headers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestDiagnostics {
    /// When the Envoy worker first saw the request.
    pub received_at: Instant,
    /// Time spent copying the body out of Envoy's buffers.
    pub body_copy_time: Duration,
    /// Number of body callbacks Envoy delivered.
    pub body_callbacks: u64,
    /// When the request was handed to the runtime.
    pub submitted_at: Option<Instant>,
}

impl RequestDiagnostics {
    /// Starts collecting timings for a request received at `received_at`.
    #[must_use]
    pub fn new(received_at: Instant) -> Self {
        Self {
            received_at,
            body_copy_time: Duration::ZERO,
            body_callbacks: 0,
            submitted_at: None,
        }
    }
}
