/// A complete response, produced by the serial runtime and by test helpers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Response {
    /// HTTP status code.
    pub status: u16,
    /// Response headers, repeated names preserved.
    pub headers: Vec<(String, String)>,
    /// Fully collected response body.
    pub body: Vec<u8>,
    /// `object_id` of the Ruby thread that ran the application.
    pub ruby_thread_object_id: u64,
}

impl Response {
    /// Returns the first header matching `name`, ignoring case.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// The response head, published before the body has been produced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResponseHead {
    /// HTTP status code.
    pub status: u16,
    /// Response headers, repeated names preserved.
    pub headers: Vec<(String, String)>,
    /// `object_id` of the Ruby thread that ran the application; zero for a
    /// response Envoy brought back from an upstream call.
    pub ruby_thread_object_id: u64,
}

/// Delivers the outcome of one buffered call back to its submitter.
#[cfg(not(loom))]
pub(crate) type Completion = Box<dyn FnOnce(Result<Response, crate::BridgeError>) + Send + 'static>;
