use thiserror::Error;

use frp_loom::error::StoreError;

/// Errors produced internally by [`InfiniteDbStore`](crate::store::InfiniteDbStore).
///
/// All variants implement `Into<StoreError>` so that they can be propagated
/// through the `frp-loom` store traits.
#[derive(Debug, Error)]
pub enum PersistenceError {
    /// An error returned by the `infinite-db` engine.
    #[error("database error: {0}")]
    Db(String),

    /// Serialization of a domain value to bytes failed.
    #[error("serialize error: {0}")]
    Serialize(String),

    /// Deserialization of bytes from the database failed.
    #[error("deserialize error: {0}")]
    Deserialize(String),
}

impl From<PersistenceError> for StoreError {
    fn from(e: PersistenceError) -> Self {
        StoreError::Io(e.to_string())
    }
}
