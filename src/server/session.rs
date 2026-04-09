use chrono::{Duration, Utc};
use dashmap::DashSet;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, TokenData, Validation};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::auth::Role;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,       // username
    pub role: String,      // role string
    pub exp: usize,        // expiry (unix timestamp)
    pub iat: usize,        // issued at
    pub jti: String,       // unique token ID for revocation
}

pub struct JwtSessionManager {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    ttl_hours: i64,
    revoked: DashSet<String>,  // revoked jti values
}

impl JwtSessionManager {
    pub fn new(ttl_hours: i64) -> Self {
        let secret = generate_secret();
        Self {
            encoding_key: EncodingKey::from_secret(&secret),
            decoding_key: DecodingKey::from_secret(&secret),
            ttl_hours,
            revoked: DashSet::new(),
        }
    }

    pub fn create_token(&self, username: &str, role: &Role) -> String {
        let now = Utc::now();
        let exp = now + Duration::hours(self.ttl_hours);
        let jti = uuid::Uuid::new_v4().to_string();

        let claims = Claims {
            sub: username.to_string(),
            role: role.to_string(),
            exp: exp.timestamp() as usize,
            iat: now.timestamp() as usize,
            jti,
        };

        encode(&Header::default(), &claims, &self.encoding_key)
            .expect("JWT encoding should not fail")
    }

    pub fn validate(&self, token: &str) -> Option<(String, Role)> {
        let token_data: TokenData<Claims> = decode(
            token,
            &self.decoding_key,
            &Validation::default(),
        ).ok()?;

        let claims = token_data.claims;

        // Check revocation
        if self.revoked.contains(&claims.jti) {
            return None;
        }

        let role = Role::from_str(&claims.role)?;
        Some((claims.sub, role))
    }

    pub fn revoke(&self, token: &str) {
        if let Ok(token_data) = decode::<Claims>(
            token,
            &self.decoding_key,
            &Validation::default(),
        ) {
            self.revoked.insert(token_data.claims.jti);
        }
    }
}

fn generate_secret() -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..64).map(|_| rng.gen::<u8>()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_validate_token() {
        let mgr = JwtSessionManager::new(24);
        let token = mgr.create_token("alice", &Role::Admin);
        let (username, role) = mgr.validate(&token).unwrap();
        assert_eq!(username, "alice");
        assert_eq!(role, Role::Admin);
    }

    #[test]
    fn test_invalid_token_rejected() {
        let mgr = JwtSessionManager::new(24);
        assert!(mgr.validate("garbage.token.here").is_none());
    }

    #[test]
    fn test_revoke_token() {
        let mgr = JwtSessionManager::new(24);
        let token = mgr.create_token("bob", &Role::ReadWrite);
        assert!(mgr.validate(&token).is_some());

        mgr.revoke(&token);
        assert!(mgr.validate(&token).is_none());
    }

    #[test]
    fn test_different_tokens_per_call() {
        let mgr = JwtSessionManager::new(24);
        let t1 = mgr.create_token("alice", &Role::Root);
        let t2 = mgr.create_token("alice", &Role::Root);
        assert_ne!(t1, t2);
    }
}
