# frozen_string_literal: true

# Generates the TLS assets shared by every benchmark architecture.
#
# All servers must present the same certificate with the same key type and size:
# handshake cost depends on both, so letting each server generate its own (Falcon
# does this by default via the localhost gem) would make the TLS numbers
# incomparable. RSA 2048 is used because Puma's MiniSSL, Ruby OpenSSL and
# BoringSSL all accept it without special configuration.

require "openssl"
require "ipaddr"

output_dir, *hosts = ARGV
abort "usage: generate_tls_assets.rb OUTPUT_DIR HOST [HOST...]" if hosts.empty?

KEY_BITS = 2048
VALIDITY_SECONDS = 24 * 60 * 60

def subject_alt_names(hosts)
  hosts.uniq.map do |host|
    IPAddr.new(host)
    "IP:#{host}"
  rescue IPAddr::InvalidAddressError
    "DNS:#{host}"
  end.join(",")
end

ca_key = OpenSSL::PKey::RSA.new(KEY_BITS)
ca_certificate = OpenSSL::X509::Certificate.new
ca_certificate.version = 2
ca_certificate.serial = 1
ca_certificate.subject = OpenSSL::X509::Name.parse("/CN=ruvoy-benchmark-ca")
ca_certificate.issuer = ca_certificate.subject
ca_certificate.public_key = ca_key.public_key
ca_certificate.not_before = Time.now - 60
ca_certificate.not_after = Time.now + VALIDITY_SECONDS

ca_extensions = OpenSSL::X509::ExtensionFactory.new
ca_extensions.subject_certificate = ca_certificate
ca_extensions.issuer_certificate = ca_certificate
ca_certificate.add_extension(ca_extensions.create_extension("basicConstraints", "CA:TRUE", true))
ca_certificate.add_extension(ca_extensions.create_extension("keyUsage", "cRLSign,keyCertSign", true))
ca_certificate.sign(ca_key, OpenSSL::Digest.new("SHA256"))

server_key = OpenSSL::PKey::RSA.new(KEY_BITS)
server_certificate = OpenSSL::X509::Certificate.new
server_certificate.version = 2
server_certificate.serial = 2
server_certificate.subject = OpenSSL::X509::Name.parse("/CN=ruvoy-benchmark")
server_certificate.issuer = ca_certificate.subject
server_certificate.public_key = server_key.public_key
server_certificate.not_before = Time.now - 60
server_certificate.not_after = Time.now + VALIDITY_SECONDS

server_extensions = OpenSSL::X509::ExtensionFactory.new
server_extensions.subject_certificate = server_certificate
server_extensions.issuer_certificate = ca_certificate
server_certificate.add_extension(server_extensions.create_extension("basicConstraints", "CA:FALSE", true))
server_certificate.add_extension(
  server_extensions.create_extension("keyUsage", "digitalSignature,keyEncipherment", true)
)
server_certificate.add_extension(
  server_extensions.create_extension("extendedKeyUsage", "serverAuth")
)
server_certificate.add_extension(
  server_extensions.create_extension("subjectAltName", subject_alt_names(hosts))
)
server_certificate.sign(ca_key, OpenSSL::Digest.new("SHA256"))

File.write(File.join(output_dir, "ca.pem"), ca_certificate.to_pem)
File.write(File.join(output_dir, "cert.pem"), server_certificate.to_pem)
File.write(File.join(output_dir, "key.pem"), server_key.to_pem, perm: 0o600)
puts "key_bits=#{KEY_BITS} key_type=RSA hosts=#{hosts.uniq.join(',')}"
