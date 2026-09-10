# frozen_string_literal: true

require "fileutils"
require "openssl"

# A throwaway CA with a server certificate and a client certificate, written as
# PEM files for Envoy to load.
module Certificates
  CLIENT_URI_SAN = "spiffe://ruvoy.test/client"
  CLIENT_DNS_SAN = "client.ruvoy.test"
  CLIENT_SUBJECT = "CN=ruvoy-test-client"

  module_function

  def write(directory)
    FileUtils.mkdir_p(directory)
    ca_key = OpenSSL::PKey::EC.generate("prime256v1")
    ca = certificate(ca_key, "/CN=ruvoy-test-ca", ca_key, nil) do |extensions|
      [ extensions.create_extension("basicConstraints", "CA:TRUE", true),
        extensions.create_extension("keyUsage", "keyCertSign,cRLSign", true) ]
    end
    server_key = OpenSSL::PKey::EC.generate("prime256v1")
    server = leaf(server_key, "/CN=localhost", ca, ca_key, "serverAuth", "DNS:localhost,IP:127.0.0.1")
    client_key = OpenSSL::PKey::EC.generate("prime256v1")
    client = leaf(client_key, "/#{CLIENT_SUBJECT}", ca, ca_key, "clientAuth",
                  "URI:#{CLIENT_URI_SAN},DNS:#{CLIENT_DNS_SAN}")

    { "ca.pem" => ca.to_pem, "server.pem" => server.to_pem, "server-key.pem" => server_key.to_pem,
      "client.pem" => client.to_pem, "client-key.pem" => client_key.to_pem }.each do |name, pem|
      File.write(File.join(directory, name), pem, perm: 0o600)
    end
    directory
  end

  def leaf(key, subject, ca, ca_key, usage, alt_names)
    certificate(key, subject, ca_key, ca) do |extensions|
      [ extensions.create_extension("basicConstraints", "CA:FALSE", true),
        extensions.create_extension("keyUsage", "digitalSignature", true),
        extensions.create_extension("extendedKeyUsage", usage),
        extensions.create_extension("subjectAltName", alt_names) ]
    end
  end

  def certificate(key, subject, signing_key, issuer)
    certificate = OpenSSL::X509::Certificate.new
    certificate.version = 2
    certificate.serial = OpenSSL::BN.rand(64)
    certificate.subject = OpenSSL::X509::Name.parse(subject)
    certificate.issuer = issuer ? issuer.subject : certificate.subject
    certificate.public_key = key
    certificate.not_before = Time.now - 60
    certificate.not_after = Time.now + 24 * 60 * 60
    extensions = OpenSSL::X509::ExtensionFactory.new
    extensions.subject_certificate = certificate
    extensions.issuer_certificate = issuer || certificate
    yield(extensions).each { |extension| certificate.add_extension(extension) }
    certificate.sign(signing_key, OpenSSL::Digest.new("SHA256"))
    certificate
  end
end
