use chrono::Utc;
use serde_json::Value;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use super::database::DatabaseManager;
use super::error::VantaError;

const MAGIC: &[u8; 4] = b"VBAK";
const FORMAT_VERSION: u8 = 1;

/// Metadata about a backup file.
#[derive(Debug, Clone)]
pub struct BackupInfo {
    pub path: String,
    pub size_bytes: u64,
    pub timestamp: String,
}

/// Result of a successful backup creation.
#[derive(Debug)]
pub struct BackupResult {
    pub path: String,
    pub size_bytes: u64,
    pub database_count: usize,
    pub document_count: usize,
    pub timestamp: String,
}

/// Create a point-in-time backup of all databases.
///
/// Format:
///   [magic: 4B "VBAK"] [version: 1B] [timestamp_len: 2B LE] [timestamp: var]
///   [db_count: 4B LE]
///   for each database:
///     [name_len: 2B LE] [name: var]
///     [col_count: 4B LE]
///     for each collection:
///       [name_len: 2B LE] [name: var]
///       [doc_count: 4B LE]
///       for each document:
///         [json_len: 4B LE] [json: var]
///   [crc32c: 4B LE] (over everything before it)
pub fn create_backup(
    db_manager: &DatabaseManager,
    data_dir: &Path,
) -> Result<BackupResult, VantaError> {
    let backup_dir = data_dir.join("backups");
    fs::create_dir_all(&backup_dir)?;

    let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
    let filename = format!("{}.vbak", timestamp);
    let path = backup_dir.join(&filename);

    let mut buf: Vec<u8> = Vec::new();
    let mut total_docs = 0usize;

    // Header
    buf.extend_from_slice(MAGIC);
    buf.push(FORMAT_VERSION);
    let ts_bytes = timestamp.as_bytes();
    buf.extend_from_slice(&(ts_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(ts_bytes);

    // Databases
    let databases = db_manager.list_databases();
    buf.extend_from_slice(&(databases.len() as u32).to_le_bytes());

    for db_name in &databases {
        // Database name
        let name_bytes = db_name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);

        // Collections
        let collections = db_manager.list_collections(db_name)?;
        buf.extend_from_slice(&(collections.len() as u32).to_le_bytes());

        for col_name in &collections {
            let col_bytes = col_name.as_bytes();
            buf.extend_from_slice(&(col_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(col_bytes);

            // Documents
            let docs = db_manager.find_all(db_name, col_name)?;
            buf.extend_from_slice(&(docs.len() as u32).to_le_bytes());

            for doc in &docs {
                let json = serde_json::to_vec(doc)
                    .map_err(|e| VantaError::Serialization(e.to_string()))?;
                buf.extend_from_slice(&(json.len() as u32).to_le_bytes());
                buf.extend_from_slice(&json);
                total_docs += 1;
            }
        }
    }

    // CRC32C checksum over everything
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    // Write to file
    let mut file = fs::File::create(&path)?;
    file.write_all(&buf)?;
    file.sync_all()?;

    let size = buf.len() as u64;

    Ok(BackupResult {
        path: path.to_string_lossy().to_string(),
        size_bytes: size,
        database_count: databases.len(),
        document_count: total_docs,
        timestamp,
    })
}

/// Restore databases from a backup file.
/// Collections present in the backup are wiped and replaced.
/// Collections not in the backup are left untouched.
pub fn restore_backup(
    db_manager: &DatabaseManager,
    backup_path: &str,
) -> Result<(), VantaError> {
    let data = fs::read(backup_path)?;

    if data.len() < 4 + 1 + 2 + 4 + 4 {
        return Err(VantaError::ValidationFailed {
            errors: vec!["Backup file too small".to_string()],
        });
    }

    // Verify CRC32C
    let payload = &data[..data.len() - 4];
    let stored_crc = u32::from_le_bytes([
        data[data.len() - 4],
        data[data.len() - 3],
        data[data.len() - 2],
        data[data.len() - 1],
    ]);
    let computed_crc = crc32c::crc32c(payload);
    if stored_crc != computed_crc {
        return Err(VantaError::ValidationFailed {
            errors: vec![format!(
                "CRC32C mismatch: expected {:08x}, got {:08x}",
                stored_crc, computed_crc
            )],
        });
    }

    let mut cursor = 0usize;

    // Magic
    if &data[cursor..cursor + 4] != MAGIC {
        return Err(VantaError::ValidationFailed {
            errors: vec!["Invalid backup magic bytes".to_string()],
        });
    }
    cursor += 4;

    // Version
    let version = data[cursor];
    if version != FORMAT_VERSION {
        return Err(VantaError::ValidationFailed {
            errors: vec![format!("Unsupported backup version: {}", version)],
        });
    }
    cursor += 1;

    // Timestamp (skip)
    let ts_len = u16::from_le_bytes([data[cursor], data[cursor + 1]]) as usize;
    cursor += 2 + ts_len;

    // Database count
    let db_count = read_u32(&data, &mut cursor) as usize;

    for _ in 0..db_count {
        let db_name = read_string16(&data, &mut cursor)?;

        // Create database if it doesn't exist
        if !db_manager.database_exists(&db_name) {
            db_manager.create_database(&db_name)?;
        }

        let col_count = read_u32(&data, &mut cursor) as usize;

        for _ in 0..col_count {
            let col_name = read_string16(&data, &mut cursor)?;

            // Drop and recreate collection to wipe existing data
            if db_manager.list_collections(&db_name)?.contains(&col_name) {
                db_manager.drop_collection(&db_name, &col_name)?;
            }
            db_manager.create_collection(&db_name, &col_name)?;

            let doc_count = read_u32(&data, &mut cursor) as usize;

            for _ in 0..doc_count {
                let json_len = read_u32(&data, &mut cursor) as usize;
                let json_bytes = &data[cursor..cursor + json_len];
                cursor += json_len;

                let doc: Value = serde_json::from_slice(json_bytes)
                    .map_err(|e| VantaError::Serialization(e.to_string()))?;
                db_manager.insert(&db_name, &col_name, doc)?;
            }
        }
    }

    Ok(())
}

/// List all backup files in the backups directory.
pub fn list_backups(data_dir: &Path) -> Vec<BackupInfo> {
    let backup_dir = data_dir.join("backups");
    let mut backups = Vec::new();

    if let Ok(entries) = fs::read_dir(&backup_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "vbak") {
                if let Ok(meta) = fs::metadata(&path) {
                    let filename = path.file_stem()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_default();
                    backups.push(BackupInfo {
                        path: path.to_string_lossy().to_string(),
                        size_bytes: meta.len(),
                        timestamp: filename,
                    });
                }
            }
        }
    }

    backups.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    backups
}

fn read_u32(data: &[u8], cursor: &mut usize) -> u32 {
    let val = u32::from_le_bytes([
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
    ]);
    *cursor += 4;
    val
}

fn read_string16(data: &[u8], cursor: &mut usize) -> Result<String, VantaError> {
    let len = u16::from_le_bytes([data[*cursor], data[*cursor + 1]]) as usize;
    *cursor += 2;
    let s = std::str::from_utf8(&data[*cursor..*cursor + len])
        .map_err(|e| VantaError::Serialization(e.to_string()))?
        .to_string();
    *cursor += len;
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, DatabaseManager) {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("data");
        std::fs::create_dir_all(&data_path).unwrap();
        let mgr = DatabaseManager::new(&data_path).unwrap();
        (dir, mgr)
    }

    #[test]
    fn test_backup_and_restore() {
        let (dir, mgr) = setup();

        // Create data
        mgr.create_database("testdb").unwrap();
        mgr.create_collection("testdb", "users").unwrap();
        mgr.insert("testdb", "users", serde_json::json!({"name": "alice", "age": 30})).unwrap();
        mgr.insert("testdb", "users", serde_json::json!({"name": "bob", "age": 25})).unwrap();

        // Create backup
        let result = create_backup(&mgr, dir.path()).unwrap();
        assert_eq!(result.database_count, 1);
        assert_eq!(result.document_count, 2);
        assert!(result.size_bytes > 0);

        // Verify backup file exists
        let backups = list_backups(dir.path());
        assert_eq!(backups.len(), 1);

        // Create a fresh DatabaseManager and restore
        let restore_path = dir.path().join("restore_data");
        std::fs::create_dir_all(&restore_path).unwrap();
        let mgr2 = DatabaseManager::new(&restore_path).unwrap();

        restore_backup(&mgr2, &result.path).unwrap();

        // Verify restored data
        let dbs = mgr2.list_databases();
        assert!(dbs.contains(&"testdb".to_string()));

        let docs = mgr2.find_all("testdb", "users").unwrap();
        assert_eq!(docs.len(), 2);

        let names: Vec<&str> = docs.iter()
            .filter_map(|d| d.get("name").and_then(|v| v.as_str()))
            .collect();
        assert!(names.contains(&"alice"));
        assert!(names.contains(&"bob"));
    }

    #[test]
    fn test_backup_crc_integrity() {
        let (dir, mgr) = setup();
        mgr.create_database("db1").unwrap();
        mgr.create_collection("db1", "col1").unwrap();
        mgr.insert("db1", "col1", serde_json::json!({"x": 1})).unwrap();

        let result = create_backup(&mgr, dir.path()).unwrap();

        // Corrupt the file
        let mut data = fs::read(&result.path).unwrap();
        data[10] ^= 0xFF; // flip a byte
        fs::write(&result.path, &data).unwrap();

        let restore_path = dir.path().join("bad_restore");
        std::fs::create_dir_all(&restore_path).unwrap();
        let mgr2 = DatabaseManager::new(&restore_path).unwrap();
        let err = restore_backup(&mgr2, &result.path);
        assert!(err.is_err());
    }

    #[test]
    fn test_empty_backup() {
        let (dir, mgr) = setup();
        // No databases — backup should succeed with 0 databases
        let result = create_backup(&mgr, dir.path()).unwrap();
        assert_eq!(result.database_count, 0);
        assert_eq!(result.document_count, 0);
    }
}
