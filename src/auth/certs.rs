use chrono::Utc;
use rcgen::{
    CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, BasicConstraints,
    SanType, Certificate,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::StorageEngine;

const CERT_TABLE: &str = "_vanta_certs";

/// Metadata for an issued client certificate (persisted).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertInfo {
    pub serial: String,
    pub username: String,
    pub issued_at: String,
    pub revoked: bool,
}

/// Manages CA keypair, server certs, and client cert issuance/revocation.
pub struct CertManager {
    ca_key: KeyPair,
    ca_cert: Certificate,
    ca_cert_pem: String,
    certs_dir: PathBuf,
    engine: Arc<StorageEngine>,
}

impl CertManager {
    /// Bootstrap or load the CA and server certificates.
    ///
    /// On first run, generates CA + server certs and writes them to `<data_dir>/certs/`.
    /// On subsequent runs, loads existing certs from disk.
    pub fn bootstrap(data_dir: &Path, engine: Arc<StorageEngine>) -> io::Result<Self> {
        let certs_dir = data_dir.join("certs");

        if !engine.table_exists(CERT_TABLE) {
            engine.create_table(CERT_TABLE)?;
        }

        let ca_cert_path = certs_dir.join("ca.pem");
        let ca_key_path = certs_dir.join("ca-key.pem");
        let server_cert_path = certs_dir.join("server.pem");
        let server_key_path = certs_dir.join("server-key.pem");

        if ca_cert_path.exists() && ca_key_path.exists() {
            // Load existing CA
            let ca_key_pem = fs::read_to_string(&ca_key_path)?;
            let ca_cert_pem = fs::read_to_string(&ca_cert_path)?;

            let ca_key = KeyPair::from_pem(&ca_key_pem)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            let ca_params = Self::ca_params();
            let ca_cert = ca_params.self_signed(&ca_key)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

            return Ok(Self {
                ca_key,
                ca_cert,
                ca_cert_pem,
                certs_dir,
                engine,
            });
        }

        // First run — generate everything
        fs::create_dir_all(&certs_dir)?;

        // Generate CA
        let ca_key = KeyPair::generate()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let ca_params = Self::ca_params();
        let ca_cert = ca_params.self_signed(&ca_key)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let ca_cert_pem = ca_cert.pem();
        let ca_key_pem = ca_key.serialize_pem();

        fs::write(&ca_cert_path, &ca_cert_pem)?;
        fs::write(&ca_key_path, &ca_key_pem)?;

        // Generate server cert signed by CA
        let server_key = KeyPair::generate()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let mut server_params = CertificateParams::new(vec![
            "localhost".to_string(),
        ]).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        server_params.distinguished_name = {
            let mut dn = DistinguishedName::new();
            dn.push(DnType::CommonName, "VantaDB Server");
            dn.push(DnType::OrganizationName, "VantaDB");
            dn
        };
        server_params.subject_alt_names = vec![
            SanType::DnsName("localhost".try_into().unwrap()),
            SanType::IpAddress("127.0.0.1".parse().unwrap()),
            SanType::IpAddress("0.0.0.0".parse().unwrap()),
        ];

        let server_cert = server_params
            .signed_by(&server_key, &ca_cert, &ca_key)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        fs::write(&server_cert_path, server_cert.pem())?;
        fs::write(&server_key_path, server_key.serialize_pem())?;

        Ok(Self {
            ca_key,
            ca_cert,
            ca_cert_pem,
            certs_dir,
            engine,
        })
    }

    fn ca_params() -> CertificateParams {
        let mut params = CertificateParams::default();
        params.distinguished_name = {
            let mut dn = DistinguishedName::new();
            dn.push(DnType::CommonName, "VantaDB CA");
            dn.push(DnType::OrganizationName, "VantaDB");
            dn
        };
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        // 10 year validity
        params.not_before = time::OffsetDateTime::now_utc();
        params.not_after = time::OffsetDateTime::now_utc()
            + time::Duration::days(365 * 10);
        params
    }

    /// Get the CA certificate PEM for client distribution.
    pub fn ca_cert_pem(&self) -> &str {
        &self.ca_cert_pem
    }

    /// Get server cert and key PEM for TLS config.
    pub fn server_cert_pem(&self) -> io::Result<String> {
        fs::read_to_string(self.certs_dir.join("server.pem"))
    }

    pub fn server_key_pem(&self) -> io::Result<String> {
        fs::read_to_string(self.certs_dir.join("server-key.pem"))
    }

    /// Issue a client certificate for a user.
    pub fn issue_cert(&self, username: &str) -> io::Result<(String, String, String)> {
        let client_key = KeyPair::generate()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut params = CertificateParams::default();
        params.distinguished_name = {
            let mut dn = DistinguishedName::new();
            dn.push(DnType::CommonName, username);
            dn.push(DnType::OrganizationName, "VantaDB");
            dn
        };
        // 1 year validity
        params.not_before = time::OffsetDateTime::now_utc();
        params.not_after = time::OffsetDateTime::now_utc()
            + time::Duration::days(365);

        let client_cert = params
            .signed_by(&client_key, &self.ca_cert, &self.ca_key)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let cert_pem = client_cert.pem();
        let key_pem = client_key.serialize_pem();

        // Generate a serial from the cert's DER hash
        let der = client_cert.der();
        let serial = format!("{:x}", crc32c::crc32c(der));

        // Persist metadata
        let info = CertInfo {
            serial: serial.clone(),
            username: username.to_string(),
            issued_at: Utc::now().to_rfc3339(),
            revoked: false,
        };
        let data = serde_json::to_vec(&info)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.engine.put(CERT_TABLE, &serial, &data)?;

        Ok((cert_pem, key_pem, serial))
    }

    /// Revoke a certificate by serial number.
    pub fn revoke_cert(&self, serial: &str) -> io::Result<bool> {
        let data = match self.engine.get(CERT_TABLE, serial) {
            Some(d) => d,
            None => return Ok(false),
        };
        let mut info: CertInfo = serde_json::from_slice(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        info.revoked = true;
        let new_data = serde_json::to_vec(&info)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.engine.put(CERT_TABLE, serial, &new_data)?;
        Ok(true)
    }

    /// Check if a certificate serial is revoked.
    pub fn is_revoked(&self, serial: &str) -> bool {
        self.engine
            .get(CERT_TABLE, serial)
            .and_then(|data| serde_json::from_slice::<CertInfo>(&data).ok())
            .map(|info| info.revoked)
            .unwrap_or(false)
    }

    /// List all issued certificates.
    pub fn list_certs(&self) -> Vec<CertInfo> {
        self.engine
            .list_keys(CERT_TABLE)
            .into_iter()
            .filter_map(|key| {
                let data = self.engine.get(CERT_TABLE, &key)?;
                serde_json::from_slice(&data).ok()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<StorageEngine>) {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        (dir, engine)
    }

    #[test]
    fn test_bootstrap_creates_ca_and_server_certs() {
        let (dir, engine) = setup();
        let mgr = CertManager::bootstrap(dir.path(), engine).unwrap();

        let certs_dir = dir.path().join("certs");
        assert!(certs_dir.join("ca.pem").exists());
        assert!(certs_dir.join("ca-key.pem").exists());
        assert!(certs_dir.join("server.pem").exists());
        assert!(certs_dir.join("server-key.pem").exists());
        assert!(mgr.ca_cert_pem().contains("BEGIN CERTIFICATE"));
    }

    #[test]
    fn test_bootstrap_loads_existing() {
        let (dir, engine) = setup();
        // First bootstrap
        let _mgr1 = CertManager::bootstrap(dir.path(), Arc::clone(&engine)).unwrap();
        let pem1 = fs::read_to_string(dir.path().join("certs/ca.pem")).unwrap();

        // Second bootstrap — should load, not regenerate
        let mgr2 = CertManager::bootstrap(dir.path(), engine).unwrap();
        let pem2 = mgr2.ca_cert_pem().to_string();

        // CA cert PEM should be the same (loaded from disk)
        assert_eq!(pem1, pem2);
    }

    #[test]
    fn test_issue_and_revoke_cert() {
        let (dir, engine) = setup();
        let mgr = CertManager::bootstrap(dir.path(), engine).unwrap();

        let (cert_pem, key_pem, serial) = mgr.issue_cert("alice").unwrap();
        assert!(cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(key_pem.contains("BEGIN"));
        assert!(!serial.is_empty());

        // Not revoked initially
        assert!(!mgr.is_revoked(&serial));

        // Revoke
        assert!(mgr.revoke_cert(&serial).unwrap());
        assert!(mgr.is_revoked(&serial));

        // Revoking non-existent serial returns false
        assert!(!mgr.revoke_cert("nonexistent").unwrap());
    }

    #[test]
    fn test_list_certs() {
        let (dir, engine) = setup();
        let mgr = CertManager::bootstrap(dir.path(), engine).unwrap();

        assert!(mgr.list_certs().is_empty());

        mgr.issue_cert("alice").unwrap();
        mgr.issue_cert("bob").unwrap();

        let certs = mgr.list_certs();
        assert_eq!(certs.len(), 2);

        let usernames: Vec<&str> = certs.iter().map(|c| c.username.as_str()).collect();
        assert!(usernames.contains(&"alice"));
        assert!(usernames.contains(&"bob"));
    }
}
