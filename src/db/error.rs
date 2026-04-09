use std::fmt;
use std::io;

/// Unified error type for all VantaDB database operations.
///
/// Replaces the previous `io::Result<Result<T, String>>` double-wrapping pattern.
#[derive(Debug)]
pub enum VantaError {
    /// Entity not found (database, collection, document, user, index)
    NotFound {
        entity: &'static str,
        name: String,
    },
    /// Entity already exists
    AlreadyExists {
        entity: &'static str,
        name: String,
    },
    /// Schema or input validation failed
    ValidationFailed {
        errors: Vec<String>,
    },
    /// Insufficient permissions
    PermissionDenied {
        required: String,
    },
    /// Transaction aborted due to serialization conflict
    TransactionConflict {
        tx_id: String,
        reason: String,
    },
    /// Transaction deadlock detected
    Deadlock {
        tx_id: String,
    },
    /// Transaction timed out
    TransactionTimeout {
        tx_id: String,
    },
    /// Underlying I/O error
    Io(io::Error),
    /// Serialization/deserialization error
    Serialization(String),
    /// Unknown or internal error
    Internal(String),
}

impl fmt::Display for VantaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VantaError::NotFound { entity, name } => {
                write!(f, "{} '{}' not found", entity, name)
            }
            VantaError::AlreadyExists { entity, name } => {
                write!(f, "{} '{}' already exists", entity, name)
            }
            VantaError::ValidationFailed { errors } => {
                write!(f, "Validation failed: {}", errors.join("; "))
            }
            VantaError::PermissionDenied { required } => {
                write!(f, "Permission denied: {} required", required)
            }
            VantaError::TransactionConflict { tx_id, reason } => {
                write!(f, "Transaction '{}' conflict: {}", tx_id, reason)
            }
            VantaError::Deadlock { tx_id } => {
                write!(f, "Transaction '{}' deadlocked", tx_id)
            }
            VantaError::TransactionTimeout { tx_id } => {
                write!(f, "Transaction '{}' timed out", tx_id)
            }
            VantaError::Io(e) => write!(f, "I/O error: {}", e),
            VantaError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            VantaError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for VantaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            VantaError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for VantaError {
    fn from(e: io::Error) -> Self {
        VantaError::Io(e)
    }
}

impl From<bincode::Error> for VantaError {
    fn from(e: bincode::Error) -> Self {
        VantaError::Serialization(e.to_string())
    }
}

impl From<serde_json::Error> for VantaError {
    fn from(e: serde_json::Error) -> Self {
        VantaError::Serialization(e.to_string())
    }
}

impl VantaError {
    pub fn to_grpc_status(&self) -> tonic::Status {
        match self {
            VantaError::NotFound { .. } => tonic::Status::not_found(self.to_string()),
            VantaError::AlreadyExists { .. } => tonic::Status::already_exists(self.to_string()),
            VantaError::ValidationFailed { .. } => {
                tonic::Status::invalid_argument(self.to_string())
            }
            VantaError::PermissionDenied { .. } => {
                tonic::Status::permission_denied(self.to_string())
            }
            VantaError::TransactionConflict { .. } => tonic::Status::aborted(self.to_string()),
            VantaError::Deadlock { .. } => tonic::Status::aborted(self.to_string()),
            VantaError::TransactionTimeout { .. } => {
                tonic::Status::deadline_exceeded(self.to_string())
            }
            VantaError::Io(_) => tonic::Status::internal(self.to_string()),
            VantaError::Serialization(_) => tonic::Status::internal(self.to_string()),
            VantaError::Internal(_) => tonic::Status::internal(self.to_string()),
        }
    }
}
