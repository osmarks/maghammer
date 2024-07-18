use derive_more::Display;

#[derive(Debug, Display)]
pub enum Error {
    Internal(anyhow::Error),
    PGError(tokio_postgres::Error),
    PGPoolError(deadpool_postgres::PoolError),
    NotFound
}

impl std::error::Error for Error {}

impl From<anyhow::Error> for Error {
    fn from(value: anyhow::Error) -> Self {
        Self::Internal(value)
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(value: tokio_postgres::Error) -> Self {
        Self::PGError(value)
    }
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(value: deadpool_postgres::PoolError) -> Self {
        Self::PGPoolError(value)
    }
}

impl ntex::web::error::WebResponseError for Error {}

pub type EResult<T> = Result<T, Error>;