// Echo server/client example using Quinn
// Run with: cargo run --example echo server | cargo run --example echo client

use std::{error::Error, net::SocketAddr, sync::Arc};

use quinn::{ClientConfig, Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

// Helper function to generate self-signed certificate
fn generate_self_signed_cert() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>), Box<dyn Error>> {
	let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
	let cert_der = CertificateDer::from(cert.cert);
	let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
	let key = PrivateKeyDer::Pkcs8(key_der);
	Ok((cert_der, key))
}

// Skip certificate verification for demo purposes (don't use in production!)
#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
	fn new() -> Arc<Self> {
		Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
	}
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
	fn verify_server_cert(
		&self,
		_end_entity: &CertificateDer<'_>,
		_intermediates: &[CertificateDer<'_>],
		_server_name: &rustls::pki_types::ServerName<'_>,
		_ocsp: &[u8],
		_now: rustls::pki_types::UnixTime,
	) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
		Ok(rustls::client::danger::ServerCertVerified::assertion())
	}

	fn verify_tls12_signature(
		&self,
		message: &[u8],
		cert: &CertificateDer<'_>,
		dss: &rustls::DigitallySignedStruct,
	) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
		rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
	}

	fn verify_tls13_signature(
		&self,
		message: &[u8],
		cert: &CertificateDer<'_>,
		dss: &rustls::DigitallySignedStruct,
	) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
		rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
	}

	fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
		self.0.signature_verification_algorithms.supported_schemes()
	}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	let args: Vec<String> = std::env::args().collect();
	if args.len() < 2 {
		eprintln!("Usage: {} [server|client]", args[0]);
		return Ok(());
	}
	match args[1].as_str() {
		"server" => run_server().await?,
		"client" => run_client().await?,
		_ => eprintln!("Unknown mode: {}", args[1]),
	}
	Ok(())
}

async fn run_server() -> Result<(), Box<dyn Error>> {
	let addr: SocketAddr = "127.0.0.1:5000".parse()?;
	let (cert, key) = generate_self_signed_cert()?;
	let server_config = ServerConfig::with_single_cert(vec![cert], key)?;
	let endpoint = Endpoint::server(server_config, addr)?;
	println!("Server listening on {}", addr);

	while let Some(incoming) = endpoint.accept().await {
		tokio::spawn(async move {
			let connection = incoming.await.unwrap();
			println!("Connection established from {}", connection.remote_address());

			while let Ok((mut send, mut recv)) = connection.accept_bi().await {
				match recv.read_to_end(1024).await {
					Ok(data) => {
						println!("Received {} bytes", data.len());
						send.write_all(&data).await.unwrap();
						send.finish().unwrap();
					}
					Err(e) => {
						eprintln!("Read error: {}", e);
						break;
					}
				}
			}
		});
	}
	Ok(())
}

async fn run_client() -> Result<(), Box<dyn Error>> {
	let server_addr: SocketAddr = "127.0.0.1:5000".parse()?;
	let endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;

	// Skip certificate verification for this demo (don't do this in production!)
	let crypto = rustls::ClientConfig::builder()
		.dangerous()
		.with_custom_certificate_verifier(SkipServerVerification::new())
		.with_no_client_auth();

	let client_config = ClientConfig::new(Arc::new(quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?));
	endpoint.set_default_client_config(client_config);

	let connection = endpoint.connect(server_addr, "localhost")?.await?;
	println!("Connected to server");

	let (mut send, mut recv) = connection.open_bi().await?;
	let msg = b"hello quinn!";
	send.write_all(msg).await?;
	send.finish()?;

	let response = recv.read_to_end(1024).await?;
	println!("Received: {}", String::from_utf8_lossy(&response));

	connection.close(0u32.into(), b"done");
	endpoint.wait_idle().await;

	Ok(())
}
