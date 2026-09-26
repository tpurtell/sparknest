use serde::{Deserialize, Serialize};

/// Errors that cross crate and wire boundaries. Each maps to one errno for
/// the FUSE frontend; keep variants coarse and meaningful to applications.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum NestError {
    #[error("not found")]
    NotFound,
    #[error("already exists")]
    Exists,
    #[error("not a directory")]
    NotDir,
    #[error("is a directory")]
    IsDir,
    #[error("directory not empty")]
    NotEmpty,
    #[error("name too long")]
    NameTooLong,
    #[error("invalid argument: {0}")]
    Invalid(String),
    #[error("operation not permitted: {0}")]
    NotPermitted(String),
    #[error("stale generation or epoch")]
    Stale,
    #[error("busy: {0}")]
    Busy(String),
    #[error("would block")]
    WouldBlock,
    #[error("no metadata quorum")]
    NoQuorum,
    #[error("data unavailable: {0}")]
    Unavailable(String),
    #[error("no space")]
    NoSpace,
    #[error("cross-device link")]
    CrossDevice,
    #[error("I/O error: {0}")]
    Io(String),
}

pub type NestResult<T> = Result<T, NestError>;

impl NestError {
    /// The errno the FUSE frontend returns for this error.
    pub fn errno(&self) -> i32 {
        match self {
            NestError::NotFound => libc::ENOENT,
            NestError::Exists => libc::EEXIST,
            NestError::NotDir => libc::ENOTDIR,
            NestError::IsDir => libc::EISDIR,
            NestError::NotEmpty => libc::ENOTEMPTY,
            NestError::NameTooLong => libc::ENAMETOOLONG,
            NestError::Invalid(_) => libc::EINVAL,
            NestError::NotPermitted(_) => libc::EPERM,
            NestError::Stale => libc::ESTALE,
            NestError::Busy(_) => libc::EBUSY,
            NestError::WouldBlock => libc::EAGAIN,
            // Minority side of a partition: refuse mutation like a read-only fs.
            NestError::NoQuorum => libc::EROFS,
            NestError::Unavailable(_) => libc::EIO,
            NestError::NoSpace => libc::ENOSPC,
            NestError::CrossDevice => libc::EXDEV,
            NestError::Io(_) => libc::EIO,
        }
    }

    pub fn from_io(e: &std::io::Error) -> Self {
        match e.raw_os_error() {
            Some(libc::ENOENT) => NestError::NotFound,
            Some(libc::EEXIST) => NestError::Exists,
            Some(libc::ENOSPC) => NestError::NoSpace,
            Some(libc::ENOTEMPTY) => NestError::NotEmpty,
            _ => NestError::Io(e.to_string()),
        }
    }
}

impl From<std::io::Error> for NestError {
    fn from(e: std::io::Error) -> Self {
        NestError::from_io(&e)
    }
}
