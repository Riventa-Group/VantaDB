use std::sync::Arc;
use tonic::{Request, Status};

use crate::auth::Role;
use super::session::JwtSessionManager;

/// Injected into request extensions after successful token validation.
#[derive(Clone, Debug)]
pub struct AuthContext {
    pub username: String,
    pub role: Role,
}

/// Interceptor applied to the VantaDb service.
/// Validates the JWT from the "authorization" metadata key.
#[derive(Clone)]
pub struct AuthInterceptor {
    pub jwt_manager: Arc<JwtSessionManager>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let token = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.strip_prefix("Bearer ").unwrap_or(s).to_string());

        let token = token.ok_or_else(|| Status::unauthenticated("Missing authorization token"))?;

        let (username, role) = self
            .jwt_manager
            .validate(&token)
            .ok_or_else(|| Status::unauthenticated("Invalid or expired token"))?;

        req.extensions_mut().insert(AuthContext { username, role });
        Ok(req)
    }
}

/// Helper to extract AuthContext from request extensions.
pub fn extract_auth<T>(request: &Request<T>) -> Result<AuthContext, Status> {
    request
        .extensions()
        .get::<AuthContext>()
        .cloned()
        .ok_or_else(|| Status::internal("Missing auth context"))
}

/// Helper to extract auth from metadata directly (for services without interceptor).
pub fn extract_auth_from_metadata<T>(
    request: &Request<T>,
    jwt_manager: &JwtSessionManager,
) -> Result<AuthContext, Status> {
    let token = request
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.strip_prefix("Bearer ").unwrap_or(s).to_string())
        .ok_or_else(|| Status::unauthenticated("Missing authorization token"))?;

    let (username, role) = jwt_manager
        .validate(&token)
        .ok_or_else(|| Status::unauthenticated("Invalid or expired token"))?;

    Ok(AuthContext { username, role })
}
