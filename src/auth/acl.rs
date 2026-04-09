use serde::{Deserialize, Serialize};
use std::io;
use std::sync::Arc;

use crate::auth::Role;
use crate::storage::StorageEngine;

const ACL_TABLE: &str = "_vanta_acls";

/// Fine-grained permission level for a database or collection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Permission {
    None,
    ReadOnly,
    ReadWrite,
    Admin,
}

impl Permission {
    pub fn can_read(&self) -> bool {
        matches!(self, Permission::ReadOnly | Permission::ReadWrite | Permission::Admin)
    }

    pub fn can_write(&self) -> bool {
        matches!(self, Permission::ReadWrite | Permission::Admin)
    }

    pub fn can_admin(&self) -> bool {
        matches!(self, Permission::Admin)
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" => Some(Permission::None),
            "readonly" | "ro" => Some(Permission::ReadOnly),
            "readwrite" | "rw" => Some(Permission::ReadWrite),
            "admin" => Some(Permission::Admin),
            _ => None,
        }
    }
}

impl std::fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Permission::None => write!(f, "none"),
            Permission::ReadOnly => write!(f, "readonly"),
            Permission::ReadWrite => write!(f, "readwrite"),
            Permission::Admin => write!(f, "admin"),
        }
    }
}

/// ACL for a specific collection within a database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionAcl {
    pub collection: String,
    pub permission: Permission,
}

/// ACL for a database, with optional per-collection overrides.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseAcl {
    pub database: String,
    pub permission: Permission,
    pub collections: Vec<CollectionAcl>,
}

/// All ACLs for a user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAcl {
    pub username: String,
    pub databases: Vec<DatabaseAcl>,
}

/// Manages per-user, per-database, per-collection ACLs.
pub struct AclManager {
    engine: Arc<StorageEngine>,
}

impl AclManager {
    pub fn new(engine: Arc<StorageEngine>) -> io::Result<Self> {
        if !engine.table_exists(ACL_TABLE) {
            engine.create_table(ACL_TABLE)?;
        }
        Ok(Self { engine })
    }

    pub fn get_user_acl(&self, username: &str) -> Option<UserAcl> {
        let data = self.engine.get(ACL_TABLE, username)?;
        serde_json::from_slice(&data).ok()
    }

    pub fn save_user_acl(&self, acl: &UserAcl) -> io::Result<()> {
        let data = serde_json::to_vec(acl)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.engine.put(ACL_TABLE, &acl.username, &data)
    }

    pub fn delete_user_acl(&self, username: &str) -> io::Result<()> {
        self.engine.delete(ACL_TABLE, username)?;
        Ok(())
    }

    /// Set database-level permission for a user.
    pub fn set_database_permission(
        &self,
        username: &str,
        database: &str,
        permission: Permission,
    ) -> io::Result<()> {
        let mut acl = self.get_user_acl(username).unwrap_or_else(|| UserAcl {
            username: username.to_string(),
            databases: vec![],
        });

        if let Some(db_acl) = acl.databases.iter_mut().find(|d| d.database == database) {
            db_acl.permission = permission;
        } else {
            acl.databases.push(DatabaseAcl {
                database: database.to_string(),
                permission,
                collections: vec![],
            });
        }

        self.save_user_acl(&acl)
    }

    /// Set collection-level permission for a user within a database.
    pub fn set_collection_permission(
        &self,
        username: &str,
        database: &str,
        collection: &str,
        permission: Permission,
    ) -> io::Result<()> {
        let mut acl = self.get_user_acl(username).unwrap_or_else(|| UserAcl {
            username: username.to_string(),
            databases: vec![],
        });

        let db_acl = if let Some(pos) = acl.databases.iter().position(|d| d.database == database) {
            &mut acl.databases[pos]
        } else {
            acl.databases.push(DatabaseAcl {
                database: database.to_string(),
                permission: Permission::None,
                collections: vec![],
            });
            acl.databases.last_mut().unwrap()
        };

        if let Some(col_acl) = db_acl.collections.iter_mut().find(|c| c.collection == collection) {
            col_acl.permission = permission;
        } else {
            db_acl.collections.push(CollectionAcl {
                collection: collection.to_string(),
                permission,
            });
        }

        self.save_user_acl(&acl)
    }

    /// Resolve effective permission for a user on a (database, collection).
    ///
    /// Resolution order (most-specific wins):
    /// 1. Collection-level ACL if set
    /// 2. Database-level ACL if set
    /// 3. Fall back to role-based default
    pub fn resolve(
        &self,
        username: &str,
        role: &Role,
        database: &str,
        collection: Option<&str>,
    ) -> Permission {
        // Root always has full access
        if role.is_root() {
            return Permission::Admin;
        }

        if let Some(user_acl) = self.get_user_acl(username) {
            if let Some(db_acl) = user_acl.databases.iter().find(|d| d.database == database) {
                // Check collection-level first (most specific)
                if let Some(col_name) = collection {
                    if let Some(col_acl) = db_acl.collections.iter().find(|c| c.collection == col_name) {
                        return col_acl.permission.clone();
                    }
                }
                // Database-level
                return db_acl.permission.clone();
            }
        }

        // Fall back to role-based default
        role_to_permission(role)
    }

    pub fn list_user_acls(&self) -> Vec<UserAcl> {
        self.engine
            .list_keys(ACL_TABLE)
            .into_iter()
            .filter_map(|key| {
                let data = self.engine.get(ACL_TABLE, &key)?;
                serde_json::from_slice(&data).ok()
            })
            .collect()
    }
}

fn role_to_permission(role: &Role) -> Permission {
    match role {
        Role::Root => Permission::Admin,
        Role::Admin => Permission::Admin,
        Role::ReadWrite => Permission::ReadWrite,
        Role::ReadOnly => Permission::ReadOnly,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, AclManager) {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let mgr = AclManager::new(engine).unwrap();
        (dir, mgr)
    }

    #[test]
    fn test_root_always_admin() {
        let (_dir, mgr) = setup();
        let perm = mgr.resolve("root", &Role::Root, "mydb", Some("mycol"));
        assert_eq!(perm, Permission::Admin);
    }

    #[test]
    fn test_role_fallback() {
        let (_dir, mgr) = setup();
        assert_eq!(mgr.resolve("alice", &Role::ReadWrite, "db1", Some("col1")), Permission::ReadWrite);
        assert_eq!(mgr.resolve("bob", &Role::ReadOnly, "db1", Some("col1")), Permission::ReadOnly);
        assert_eq!(mgr.resolve("carol", &Role::Admin, "db1", None), Permission::Admin);
    }

    #[test]
    fn test_database_level_acl() {
        let (_dir, mgr) = setup();
        mgr.set_database_permission("alice", "db1", Permission::ReadOnly).unwrap();

        // Database ACL overrides role
        assert_eq!(mgr.resolve("alice", &Role::ReadWrite, "db1", Some("col1")), Permission::ReadOnly);
        // Other databases still use role
        assert_eq!(mgr.resolve("alice", &Role::ReadWrite, "db2", Some("col1")), Permission::ReadWrite);
    }

    #[test]
    fn test_collection_level_acl_overrides_database() {
        let (_dir, mgr) = setup();
        mgr.set_database_permission("alice", "db1", Permission::ReadOnly).unwrap();
        mgr.set_collection_permission("alice", "db1", "secret", Permission::None).unwrap();
        mgr.set_collection_permission("alice", "db1", "public", Permission::ReadWrite).unwrap();

        assert_eq!(mgr.resolve("alice", &Role::ReadWrite, "db1", Some("secret")), Permission::None);
        assert_eq!(mgr.resolve("alice", &Role::ReadWrite, "db1", Some("public")), Permission::ReadWrite);
        // Unspecified collection falls to database level
        assert_eq!(mgr.resolve("alice", &Role::ReadWrite, "db1", Some("other")), Permission::ReadOnly);
    }

    #[test]
    fn test_delete_user_acl() {
        let (_dir, mgr) = setup();
        mgr.set_database_permission("alice", "db1", Permission::Admin).unwrap();
        assert_eq!(mgr.resolve("alice", &Role::ReadOnly, "db1", None), Permission::Admin);

        mgr.delete_user_acl("alice").unwrap();
        assert_eq!(mgr.resolve("alice", &Role::ReadOnly, "db1", None), Permission::ReadOnly);
    }
}
