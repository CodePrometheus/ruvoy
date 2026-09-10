//! Copies what Envoy knows about a request into owned data for the runtime.

use crate::{
    runtime::ContextConfig,
    worker::{attribute_string, authority_host},
};
use abi::*;
use envoy_proxy_dynamic_modules_rust_sdk::*;
use ruvoy::{Connection, Context, MetadataValue, Namespace, PeerCertificate, Tls};

type Attribute = envoy_dynamic_module_type_attribute_id;
type Source = envoy_dynamic_module_type_metadata_source;

/// Reads the request's context. Envoy only answers on the worker, while one of
/// the request's callbacks runs, so this happens once, with the headers.
pub(crate) fn copy<EHF: EnvoyHttpFilter>(envoy_filter: &EHF, config: &ContextConfig) -> Context {
    Context {
        route_name: attribute_string(envoy_filter, Attribute::XdsRouteName),
        connection: connection(envoy_filter),
        tls: tls(envoy_filter),
        dynamic_metadata: metadata(envoy_filter, Source::Dynamic, &config.metadata_namespaces),
        route_metadata: metadata(envoy_filter, Source::Route, &config.metadata_namespaces),
    }
}

fn connection<EHF: EnvoyHttpFilter>(envoy_filter: &EHF) -> Connection {
    let address = |attribute: Attribute| {
        attribute_string(envoy_filter, attribute)
            .and_then(|address| authority_host(&address).map(str::to_owned))
    };
    let port = |attribute: Attribute| {
        envoy_filter
            .get_attribute_int(attribute)
            .and_then(|port| u16::try_from(port).ok())
    };
    Connection {
        id: envoy_filter
            .get_attribute_int(Attribute::ConnectionId)
            .and_then(|id| u64::try_from(id).ok()),
        source_address: address(Attribute::SourceAddress),
        source_port: port(Attribute::SourcePort),
        destination_address: address(Attribute::DestinationAddress),
        destination_port: port(Attribute::DestinationPort),
    }
}

fn tls<EHF: EnvoyHttpFilter>(envoy_filter: &EHF) -> Option<Tls> {
    let text = |attribute: Attribute| attribute_string(envoy_filter, attribute);
    let version = text(Attribute::ConnectionTlsVersion)?;
    let presented = envoy_filter
        .get_attribute_bool(Attribute::ConnectionMtls)
        .unwrap_or(false);
    Some(Tls {
        version,
        server_name: text(Attribute::ConnectionRequestedServerName),
        peer_certificate: presented.then(|| PeerCertificate {
            subject: text(Attribute::ConnectionSubjectPeerCertificate),
            uri_san: text(Attribute::ConnectionUriSanPeerCertificate),
            dns_san: text(Attribute::ConnectionDnsSanPeerCertificate),
            sha256: text(Attribute::ConnectionSha256PeerCertificateDigest),
        }),
    })
}

/// Copies the listed namespaces that exist, with the fields Envoy can read.
fn metadata<EHF: EnvoyHttpFilter>(
    envoy_filter: &EHF,
    source: Source,
    namespaces: &[String],
) -> Vec<Namespace> {
    namespaces
        .iter()
        .filter_map(|name| {
            let keys = envoy_filter.get_metadata_keys(source, name)?;
            let fields = keys
                .iter()
                .filter_map(|key| {
                    let key = String::from_utf8_lossy(key.as_slice());
                    value(envoy_filter, source, name, &key).map(|value| (key.into_owned(), value))
                })
                .collect();
            Some(Namespace {
                name: name.clone(),
                fields,
            })
        })
        .collect()
}

/// Envoy has one getter per kind, so a value is whichever getter answers.
fn value<EHF: EnvoyHttpFilter>(
    envoy_filter: &EHF,
    source: Source,
    namespace: &str,
    key: &str,
) -> Option<MetadataValue> {
    if let Some(text) = envoy_filter.get_metadata_string(source, namespace, key) {
        return Some(MetadataValue::String(
            String::from_utf8_lossy(text.as_slice()).into_owned(),
        ));
    }
    if let Some(number) = envoy_filter.get_metadata_number(source, namespace, key) {
        return Some(MetadataValue::Number(number));
    }
    if let Some(flag) = envoy_filter.get_metadata_bool(source, namespace, key) {
        return Some(MetadataValue::Bool(flag));
    }
    let size = envoy_filter.get_metadata_list_size(source, namespace, key)?;
    Some(MetadataValue::List(
        (0..size)
            .filter_map(|index| element(envoy_filter, source, namespace, key, index))
            .collect(),
    ))
}

fn element<EHF: EnvoyHttpFilter>(
    envoy_filter: &EHF,
    source: Source,
    namespace: &str,
    key: &str,
    index: usize,
) -> Option<MetadataValue> {
    envoy_filter
        .get_metadata_list_string(source, namespace, key, index)
        .map(|text| MetadataValue::String(String::from_utf8_lossy(text.as_slice()).into_owned()))
        .or_else(|| {
            envoy_filter
                .get_metadata_list_number(source, namespace, key, index)
                .map(MetadataValue::Number)
        })
        .or_else(|| {
            envoy_filter
                .get_metadata_list_bool(source, namespace, key, index)
                .map(MetadataValue::Bool)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &'static str) -> Option<EnvoyBuffer<'static>> {
        Some(EnvoyBuffer::new(value.as_bytes()))
    }

    /// A request on connection 7 that matched route `api` and carries tenant
    /// metadata; `tls` and `presented` shape the connection it arrived on.
    fn request(tls: bool, presented: bool) -> MockEnvoyHttpFilter {
        let mut envoy_filter = MockEnvoyHttpFilter::default();
        envoy_filter
            .expect_get_attribute_string()
            .returning(move |attribute| match attribute {
                Attribute::XdsRouteName => text("api"),
                Attribute::SourceAddress => text("10.0.0.7:51234"),
                Attribute::DestinationAddress => text("10.0.0.9:8443"),
                Attribute::ConnectionTlsVersion if tls => text("TLSv1.3"),
                Attribute::ConnectionRequestedServerName if tls => text("api.example.com"),
                Attribute::ConnectionSubjectPeerCertificate if presented => text("CN=client"),
                Attribute::ConnectionUriSanPeerCertificate if presented => {
                    text("spiffe://example.com/client")
                }
                Attribute::ConnectionSha256PeerCertificateDigest if presented => text("ab12"),
                _ => None,
            });
        envoy_filter
            .expect_get_attribute_int()
            .returning(|attribute| match attribute {
                Attribute::ConnectionId => Some(7),
                Attribute::SourcePort => Some(51234),
                Attribute::DestinationPort => Some(8443),
                _ => None,
            });
        envoy_filter
            .expect_get_attribute_bool()
            .returning(move |attribute| match attribute {
                Attribute::ConnectionMtls if tls => Some(presented),
                _ => None,
            });
        envoy_filter
            .expect_get_metadata_keys()
            .returning(|source, namespace| match (source, namespace) {
                (Source::Dynamic, "acme.tenant") => Some(
                    ["id", "weight", "beta", "regions", "nested"]
                        .map(|key| EnvoyBuffer::new(key.as_bytes()))
                        .to_vec(),
                ),
                (Source::Route, "acme.tenant") => Some(vec![EnvoyBuffer::new(b"tier")]),
                _ => None,
            });
        envoy_filter
            .expect_get_metadata_string()
            .returning(|source, _, key| match (source, key) {
                (Source::Dynamic, "id") => text("t-42"),
                (Source::Route, "tier") => text("gold"),
                _ => None,
            });
        envoy_filter
            .expect_get_metadata_number()
            .returning(|source, _, key| match (source, key) {
                (Source::Dynamic, "weight") => Some(3.0),
                _ => None,
            });
        envoy_filter
            .expect_get_metadata_bool()
            .returning(|source, _, key| match (source, key) {
                (Source::Dynamic, "beta") => Some(true),
                _ => None,
            });
        envoy_filter
            .expect_get_metadata_list_size()
            .returning(|source, _, key| match (source, key) {
                (Source::Dynamic, "regions") => Some(2),
                _ => None,
            });
        envoy_filter
            .expect_get_metadata_list_string()
            .returning(|_, _, key, index| match (key, index) {
                ("regions", 0) => text("us"),
                _ => None,
            });
        envoy_filter
            .expect_get_metadata_list_number()
            .returning(|_, _, key, index| match (key, index) {
                ("regions", 1) => Some(2.0),
                _ => None,
            });
        envoy_filter
            .expect_get_metadata_list_bool()
            .returning(|_, _, _, _| None);
        envoy_filter
    }

    fn config() -> ContextConfig {
        ContextConfig {
            metadata_namespaces: vec!["acme.tenant".to_owned(), "absent".to_owned()],
        }
    }

    #[test]
    fn a_plaintext_request_carries_its_route_connection_and_listed_metadata() {
        let context = copy(&request(false, false), &config());

        assert_eq!(context.route_name.as_deref(), Some("api"));
        assert_eq!(
            context.connection,
            Connection {
                id: Some(7),
                source_address: Some("10.0.0.7".to_owned()),
                source_port: Some(51234),
                destination_address: Some("10.0.0.9".to_owned()),
                destination_port: Some(8443),
            }
        );
        assert_eq!(context.tls, None);
        assert_eq!(
            context.dynamic_metadata,
            vec![Namespace {
                name: "acme.tenant".to_owned(),
                fields: vec![
                    ("id".to_owned(), MetadataValue::String("t-42".to_owned())),
                    ("weight".to_owned(), MetadataValue::Number(3.0)),
                    ("beta".to_owned(), MetadataValue::Bool(true)),
                    (
                        "regions".to_owned(),
                        MetadataValue::List(vec![
                            MetadataValue::String("us".to_owned()),
                            MetadataValue::Number(2.0),
                        ]),
                    ),
                ],
            }],
            "the nested field has no getter and the absent namespace does not exist"
        );
        assert_eq!(
            context.route_metadata,
            vec![Namespace {
                name: "acme.tenant".to_owned(),
                fields: vec![("tier".to_owned(), MetadataValue::String("gold".to_owned()))],
            }]
        );
    }

    #[test]
    fn a_client_certificate_is_reported_only_when_one_was_presented() {
        let without = copy(&request(true, false), &config()).tls;
        assert_eq!(
            without,
            Some(Tls {
                version: "TLSv1.3".to_owned(),
                server_name: Some("api.example.com".to_owned()),
                peer_certificate: None,
            })
        );

        let with = copy(&request(true, true), &config()).tls;
        assert_eq!(
            with.and_then(|tls| tls.peer_certificate),
            Some(PeerCertificate {
                subject: Some("CN=client".to_owned()),
                uri_san: Some("spiffe://example.com/client".to_owned()),
                dns_san: None,
                sha256: Some("ab12".to_owned()),
            })
        );
    }
}
