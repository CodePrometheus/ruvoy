//! What Envoy knows about a request beyond its headers.
//!
//! Envoy answers only on the worker thread, and only while one of the request's
//! callbacks is running, so the worker copies all of it into owned data when
//! the headers arrive. Ruby objects are built from that copy only when the
//! application reads them, on the one thread every request shares.

/// Envoy's view of one request, as the worker saw it when the headers arrived.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Context {
    /// Name of the matched route; absent when the route has none.
    pub route_name: Option<String>,
    /// The downstream connection the request arrived on.
    pub connection: Connection,
    /// Absent on a plaintext connection.
    pub tls: Option<Tls>,
    /// Dynamic metadata that earlier filters attached, in the configured
    /// namespaces.
    pub dynamic_metadata: Vec<Namespace>,
    /// Metadata configured on the matched route, in the configured namespaces.
    pub route_metadata: Vec<Namespace>,
}

/// Addresses of the downstream connection.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Connection {
    /// Envoy's identifier for the connection, shared by every request on it.
    pub id: Option<u64>,
    /// Address of the peer.
    pub source_address: Option<String>,
    /// Port of the peer.
    pub source_port: Option<u16>,
    /// Address the connection was accepted on.
    pub destination_address: Option<String>,
    /// Port the connection was accepted on.
    pub destination_port: Option<u16>,
}

/// What the TLS handshake established.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Tls {
    /// Negotiated protocol version, for example `TLSv1.3`.
    pub version: String,
    /// Server name the client asked for.
    pub server_name: Option<String>,
    /// The certificate the client presented, if it presented one.
    pub peer_certificate: Option<PeerCertificate>,
}

/// A client certificate, as far as Envoy reports it.
///
/// Presented is not the same as verified: these fields identify the client only
/// on a listener that requires client certificates signed by a trusted CA.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerCertificate {
    /// Subject distinguished name.
    pub subject: Option<String>,
    /// First URI subject alternative name; Envoy reports no others.
    pub uri_san: Option<String>,
    /// First DNS subject alternative name; Envoy reports no others.
    pub dns_san: Option<String>,
    /// Hex-encoded SHA-256 digest of the certificate.
    pub sha256: Option<String>,
}

/// One metadata namespace and the fields in it that Envoy can hand over.
#[derive(Clone, Debug, PartialEq)]
pub struct Namespace {
    /// Namespace name, conventionally the filter that wrote it.
    pub name: String,
    /// Readable fields. Nested structures are left out: Envoy offers modules
    /// no way to read them.
    pub fields: Vec<(String, MetadataValue)>,
}

/// A metadata value Envoy can hand over.
#[derive(Clone, Debug, PartialEq)]
pub enum MetadataValue {
    /// A string.
    String(String),
    /// A number; metadata stores every number as a double, integers included.
    Number(f64),
    /// A boolean.
    Bool(bool),
    /// A list of strings, numbers and booleans.
    List(Vec<MetadataValue>),
}
