use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Failed to open input file: {0}")]
    InputFileError(String),

    #[error("Failed to parse XML: {0}")]
    XmlParseError(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Malformed record of type {record_type}: {reason}")]
    MalformedRecord { record_type: String, reason: String },

    #[error("Failed to write CSV file: {0}")]
    CsvWriteError(String),

    #[error("Failed to create output ZIP file: {0}")]
    ZipError(String),

    #[error("Threading error: {0}")]
    ThreadError(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub type Result<T> = std::result::Result<T, AppError>;
