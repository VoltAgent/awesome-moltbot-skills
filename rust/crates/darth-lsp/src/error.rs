use std::fmt::{Display, Formatter};
use std::path::PathBuf;

#[derive(Debug)]
pub enum LspError {
    Io(std::io::Error),
    Json(serde_json::Error),
    InvalidHeader(String),
    MissingContentLength,
    InvalidContentLength(String),
    UnsupportedDocument(PathBuf),
    UnknownServer(String),
    DuplicateExtension {
        extension: String,
        existing_server: String,
        new_server: String,
    },
    PathToUrl(PathBuf),
    Protocol(String),
}

impl Display for LspError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Json(error) => write!(f, "{error}"),
            Self::InvalidHeader(header) => write!(f, "invalid LSP header: {header}"),
            Self::MissingContentLength => write!(f, "missing LSP Content-Length header"),
            Self::InvalidContentLength(value) => {
                write!(f, "invalid LSP Content-Length value: {value}")
            }
            Self::UnsupportedDocument(path) => {
                write!(f, "no LSP server configured for {}", path.display())
            }
            Self::UnknownServer(name) => write!(f, "unknown LSP server: {name}"),
            Self::DuplicateExtension {
                extension,
                existing_server,
                new_server,
            } => write!(
                f,
                "duplicate LSP extension mapping for {extension}: {existing_server} and {new_server}"
            ),
            Self::PathToUrl(path) => write!(f, "failed to convert path to file URL: {}", path.display()),
            Self::Protocol(message) => write!(f, "LSP protocol error: {message}"),
        }
    }
}

impl std::error::Error for LspError {}

impl From<std::io::Error> for LspError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for LspError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}
