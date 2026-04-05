use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io;

use crate::storage::StorageEngine;

const AUTH_TABLE: &str = "_vanta_users";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Role {
    Root,
    Admin,
    ReadWrite,
    ReadOnly,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Root => write!(f, "root"),
            Role::Admin => write!(f, "admin"),
            Role::ReadWrite => write!(f, "readwrite"),
            Role::ReadOnly => write!(f, "readonly"),
        }
    }
}

impl Role {
    pub fn from_str(s: &str) -> Option<Role> {
        match s.to_lowercase().as_str() {
            "root" => Some(Role::Root),
            "admin" => Some(Role::Admin),
            "readwrite" | "rw" => Some(Role::ReadWrite),
            "readonly" | "ro" => Some(Role::ReadOnly),
            _ => None,
        }
    }

    pub fn can_write(&self) -> bool {
        matches!(self, Role::Root | Role::Admin | Role::ReadWrite)
    }

    pub fn can_admin(&self) -> bool {
        matches!(self, Role::Root | Role::Admin)
    }

    pub fn is_root(&self) -> bool {
        matches!(self, Role::Root)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: Option<String>,
    pub role: Role,
    pub created_at: DateTime<Utc>,
    pub databases: Vec<String>, // empty = access to all (for root/admin)
}

pub struct AuthManager {
    engine: StorageEngine,
}

impl AuthManager {
    pub fn new(engine: StorageEngine) -> io::Result<Self> {
        let mgr = Self { engine };
        mgr.ensure_root()?;
        Ok(mgr)
    }

    fn ensure_root(&self) -> io::Result<()> {
        if !self.engine.table_exists(AUTH_TABLE) {
            self.engine.create_table(AUTH_TABLE)?;
        }
        if self.get_user("root")?.is_none() {
            let root = User {
                username: "root".to_string(),
                password_hash: None, // root has no password
                role: Role::Root,
                created_at: Utc::now(),
                databases: vec![],
            };
            self.save_user(&root)?;
        }
        Ok(())
    }

    fn save_user(&self, user: &User) -> io::Result<()> {
        let data = bincode::serialize(user)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.engine.put(AUTH_TABLE, &user.username, &data)
    }

    pub fn get_user(&self, username: &str) -> io::Result<Option<User>> {
        match self.engine.get(AUTH_TABLE, username) {
            Some(data) => {
                let user: User = bincode::deserialize(&data)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                Ok(Some(user))
            }
            None => Ok(None),
        }
    }

    pub fn authenticate(&self, username: &str, password: &str) -> io::Result<Option<User>> {
        let user = match self.get_user(username)? {
            Some(u) => u,
            None => return Ok(None),
        };

        // Root user needs no password
        if user.role == Role::Root && user.password_hash.is_none() {
            return Ok(Some(user));
        }

        match &user.password_hash {
            Some(hash) => {
                let parsed = PasswordHash::new(hash)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                if Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok() {
                    Ok(Some(user))
                } else {
                    Ok(None)
                }
            }
            None => {
                // No password set and not root = deny
                Ok(None)
            }
        }
    }

    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        role: Role,
        databases: Vec<String>,
    ) -> io::Result<Result<(), String>> {
        if self.get_user(username)?.is_some() {
            return Ok(Err(format!("User '{}' already exists", username)));
        }

        if role == Role::Root {
            return Ok(Err("Cannot create another root user".to_string()));
        }

        let salt = SaltString::generate(&mut OsRng);
        let password_hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
            .to_string();

        let user = User {
            username: username.to_string(),
            password_hash: Some(password_hash),
            role,
            created_at: Utc::now(),
            databases,
        };

        self.save_user(&user)?;
        Ok(Ok(()))
    }

    pub fn delete_user(&self, username: &str) -> io::Result<Result<(), String>> {
        if username == "root" {
            return Ok(Err("Cannot delete root user".to_string()));
        }
        if self.get_user(username)?.is_none() {
            return Ok(Err(format!("User '{}' not found", username)));
        }
        self.engine.delete(AUTH_TABLE, username)?;
        Ok(Ok(()))
    }

    pub fn list_users(&self) -> Vec<String> {
        self.engine.list_keys(AUTH_TABLE)
    }

    pub fn set_password(&self, username: &str, password: &str) -> io::Result<Result<(), String>> {
        let mut user = match self.get_user(username)? {
            Some(u) => u,
            None => return Ok(Err(format!("User '{}' not found", username))),
        };

        let salt = SaltString::generate(&mut OsRng);
        let password_hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
            .to_string();

        user.password_hash = Some(password_hash);
        self.save_user(&user)?;
        Ok(Ok(()))
    }
}
