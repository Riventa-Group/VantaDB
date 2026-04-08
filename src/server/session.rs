use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use uuid::Uuid;

use crate::auth::Role;

pub struct SessionInfo {
    pub username: String,
    pub role: Role,
    pub expires_at: DateTime<Utc>,
}

pub struct SessionStore {
    sessions: DashMap<String, SessionInfo>,
    ttl_hours: i64,
}

impl SessionStore {
    pub fn new(ttl_hours: i64) -> Self {
        Self {
            sessions: DashMap::new(),
            ttl_hours,
        }
    }

    pub fn create_session(&self, username: String, role: Role) -> String {
        let token = Uuid::new_v4().to_string();
        let info = SessionInfo {
            username,
            role,
            expires_at: Utc::now() + Duration::hours(self.ttl_hours),
        };
        self.sessions.insert(token.clone(), info);
        token
    }

    pub fn validate(&self, token: &str) -> Option<(String, Role)> {
        let session = self.sessions.get(token)?;
        if Utc::now() > session.expires_at {
            drop(session);
            self.sessions.remove(token);
            return None;
        }
        Some((session.username.clone(), session.role.clone()))
    }
}
