//! rustls `ServerConfig` loader for native TLS support (Part A foundation).
//!
//! This module is the **only** place in pylon that touches rustls directly.
//! It exposes two public entry points:
//!
//! - [`load_server_config`]: build an `Arc<rustls::ServerConfig>` from PEM files.
//! - [`resolve_tls`]: the high-level helper that interprets the three config knobs
//!   (`tls_cert_path`, `tls_key_path`, `tls_ca_path`) and either returns a ready
//!   `ServerConfig`, plain-mode `None`, or a fatal `Err` on misconfiguration.
//!
//! # CryptoProvider note
//! rustls 0.23 requires a process-global `CryptoProvider` to be installed before
//! any `ServerConfig` is built.  `load_server_config` calls
//! `rustls::crypto::ring::default_provider().install_default()` and silently
//! ignores the `Err(AlreadyInstalled)` return value, so the call is idempotent
//! whether reqwest/fred have already installed a provider or not.

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig as RustlsServerConfig;

/// Build a `rustls::ServerConfig` from PEM cert chain + private key files.
///
/// - `cert_path`: path to a PEM file containing one or more certificates
///   (the leaf cert first, intermediates after — the usual chain ordering).
/// - `key_path`: path to a PEM file containing a PKCS#8, PKCS#1 RSA, or
///   SEC1 EC private key.
/// - `ca_path`: when `Some`, load the CA cert and require + verify client
///   certificates (mTLS). Currently **not implemented** — passing `Some` returns a
///   clear `Err`; the code is structured so it can be wired up in the next task
///   without touching the call-site.
///
/// Returns a clear, user-readable `Err` on any failure (missing file, permission
/// error, malformed PEM, no private key found, etc.). Never panics.
pub fn load_server_config(
    cert_path: &str,
    key_path: &str,
    ca_path: Option<&str>,
) -> anyhow::Result<Arc<RustlsServerConfig>> {
    // ── 1. Ensure a CryptoProvider is installed ──────────────────────────────
    // rustls 0.23 requires a process-default `CryptoProvider` before any
    // `ServerConfig` can be built. `install_default()` is idempotent: it returns
    // `Err(Arc<CryptoProvider>)` when one is already installed, which we ignore.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // ── 2. Load the certificate chain ────────────────────────────────────────
    let cert_file = File::open(cert_path)
        .with_context(|| format!("PYLON_TLS_CERT: cannot open certificate file '{cert_path}'"))?;
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(cert_file))
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| {
                format!("PYLON_TLS_CERT: failed to parse PEM certificates from '{cert_path}'")
            })?;
    if certs.is_empty() {
        return Err(anyhow!(
            "PYLON_TLS_CERT: no certificates found in '{cert_path}'"
        ));
    }

    // ── 3. Load the private key ───────────────────────────────────────────────
    let key_file = File::open(key_path)
        .with_context(|| format!("PYLON_TLS_KEY: cannot open private key file '{key_path}'"))?;
    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut BufReader::new(key_file))
            .with_context(|| {
                format!("PYLON_TLS_KEY: failed to parse PEM private key from '{key_path}'")
            })?
            .ok_or_else(|| {
                anyhow!("PYLON_TLS_KEY: no private key found in '{key_path}'")
            })?;

    // ── 4. mTLS: not yet implemented ─────────────────────────────────────────
    if let Some(ca) = ca_path {
        return Err(anyhow!(
            "PYLON_TLS_CA: mTLS client-certificate verification is not yet implemented \
             (ca_path = '{ca}'). Remove PYLON_TLS_CA to run without mTLS, or wait for \
             the follow-up task that wires up WebPkiClientVerifier."
        ));
    }

    // ── 5. Build the ServerConfig (no client auth) ────────────────────────────
    let config = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .with_context(|| {
            format!(
                "failed to build rustls ServerConfig from cert='{cert_path}' key='{key_path}'"
            )
        })?;

    Ok(Arc::new(config))
}

/// Resolve the optional TLS configuration from the three `ServerConfig` knobs.
///
/// | `tls_cert_path` | `tls_key_path` | result                         |
/// |-----------------|----------------|-------------------------------|
/// | `Some`          | `Some`         | `Ok(Some(Arc<ServerConfig>))` |
/// | `None`          | `None`         | `Ok(None)` (plain mode)        |
/// | one set, other not | —           | `Err(…)` (fatal misconfig)     |
///
/// When `tls_ca_path` is `Some` the mTLS path is requested; this currently
/// returns an explicit error (see [`load_server_config`]).
pub fn resolve_tls(
    cert_path: &Option<String>,
    key_path: &Option<String>,
    ca_path: &Option<String>,
) -> anyhow::Result<Option<Arc<RustlsServerConfig>>> {
    match (cert_path.as_deref(), key_path.as_deref()) {
        (Some(cert), Some(key)) => {
            let cfg = load_server_config(cert, key, ca_path.as_deref())?;
            Ok(Some(cfg))
        }
        (None, None) => Ok(None),
        (Some(_), None) => Err(anyhow!(
            "PYLON_TLS_CERT is set but PYLON_TLS_KEY is missing. \
             Both must be set together to enable TLS, or neither to use plain mode."
        )),
        (None, Some(_)) => Err(anyhow!(
            "PYLON_TLS_KEY is set but PYLON_TLS_CERT is missing. \
             Both must be set together to enable TLS, or neither to use plain mode."
        )),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Generate a self-signed cert+key pair with `rcgen` and write them to
    /// temporary files under `std::env::temp_dir()`. The `tag` parameter is
    /// used to give each call-site a unique filename so parallel tests don't
    /// share or delete each other's files. Returns `(cert_path, key_path)`.
    fn generate_self_signed_cert_files(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        use rcgen::generate_simple_self_signed;
        let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
        let cert = generate_simple_self_signed(subject_alt_names)
            .expect("rcgen: failed to generate self-signed cert");

        let dir = std::env::temp_dir();
        let cert_path = dir.join(format!("pylon-test-cert-{}-{tag}.pem", std::process::id()));
        let key_path = dir.join(format!("pylon-test-key-{}-{tag}.pem", std::process::id()));

        let mut cert_file = File::create(&cert_path).expect("create temp cert file");
        cert_file
            .write_all(cert.cert.pem().as_bytes())
            .expect("write cert PEM");

        let mut key_file = File::create(&key_path).expect("create temp key file");
        key_file
            .write_all(cert.key_pair.serialize_pem().as_bytes())
            .expect("write key PEM");

        (cert_path, key_path)
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// `resolve_tls` returns `None` (plain mode) when both paths are `None`.
    #[test]
    fn resolve_tls_none_none_is_plain_mode() {
        let result = resolve_tls(&None, &None, &None);
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        assert!(result.unwrap().is_none(), "expected None (plain mode)");
    }

    /// `resolve_tls` returns an error when only `cert_path` is set.
    #[test]
    fn resolve_tls_cert_only_is_fatal_error() {
        let result = resolve_tls(&Some("/tmp/cert.pem".into()), &None, &None);
        assert!(result.is_err(), "expected Err for cert-only misconfig");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("PYLON_TLS_CERT"),
            "error should mention PYLON_TLS_CERT: {msg}"
        );
    }

    /// `resolve_tls` returns an error when only `key_path` is set.
    #[test]
    fn resolve_tls_key_only_is_fatal_error() {
        let result = resolve_tls(&None, &Some("/tmp/key.pem".into()), &None);
        assert!(result.is_err(), "expected Err for key-only misconfig");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("PYLON_TLS_KEY"),
            "error should mention PYLON_TLS_KEY: {msg}"
        );
    }

    /// `load_server_config` with a valid self-signed cert+key succeeds.
    #[test]
    fn load_server_config_valid_cert_key_returns_ok() {
        let (cert_path, key_path) = generate_self_signed_cert_files("load-ok");
        let result = load_server_config(
            cert_path.to_str().unwrap(),
            key_path.to_str().unwrap(),
            None,
        );
        // Clean up regardless of test outcome.
        let _ = std::fs::remove_file(&cert_path);
        let _ = std::fs::remove_file(&key_path);

        assert!(result.is_ok(), "expected Ok(Arc<ServerConfig>), got: {result:?}");
        // The returned Arc should be usable (non-trivial sanity check: it's not null).
        let _ = result.unwrap();
    }

    /// `load_server_config` with a nonexistent cert path returns `Err`, no panic.
    #[test]
    fn load_server_config_missing_cert_returns_err() {
        let result = load_server_config(
            "/nonexistent/path/cert.pem",
            "/nonexistent/path/key.pem",
            None,
        );
        assert!(result.is_err(), "expected Err for nonexistent paths");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("PYLON_TLS_CERT"),
            "error should mention PYLON_TLS_CERT: {msg}"
        );
    }

    /// `resolve_tls` with valid cert+key returns `Ok(Some(…))`.
    #[test]
    fn resolve_tls_valid_cert_key_returns_some() {
        let (cert_path, key_path) = generate_self_signed_cert_files("resolve-some");
        let cert_str = cert_path.to_str().unwrap().to_string();
        let key_str = key_path.to_str().unwrap().to_string();

        let result = resolve_tls(&Some(cert_str), &Some(key_str), &None);

        let _ = std::fs::remove_file(&cert_path);
        let _ = std::fs::remove_file(&key_path);

        assert!(result.is_ok(), "expected Ok(Some(…)), got: {result:?}");
        assert!(result.unwrap().is_some(), "expected Some(Arc<ServerConfig>)");
    }
}
