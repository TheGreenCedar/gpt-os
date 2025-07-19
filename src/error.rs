use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("CSV error: {0}")]
    CsvError(#[from] csv::Error),

    #[error("ZIP error: {0}")]
    ZipArchiveError(#[from] zip::result::ZipError),

    #[error("7z error: {0}")]
    SevenZError(#[from] sevenz_rust::Error),

    #[error("Thread pool build error: {0}")]
    ThreadPoolError(#[from] rayon::ThreadPoolBuildError),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl From<Box<dyn std::error::Error>> for AppError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        AppError::Unknown(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, AppError>;
