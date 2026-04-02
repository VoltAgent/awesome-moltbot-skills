use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};

use lsp_types::{Diagnostic, Range};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LspServerConfig {
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub workspace_root: PathBuf,
    pub initialization_options: Option<Value>,
    pub extension_to_language: BTreeMap<String, String>,
}

impl LspServerConfig {
    #[must_use]
    pub fn language_id_for(&self, path: &Path) -> Option<&str> {
        let extension = normalize_extension(path.extension()?.to_string_lossy().as_ref());
        self.extension_to_language
            .get(&extension)
            .map(String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileDiagnostics {
    pub path: PathBuf,
    pub uri: String,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct WorkspaceDiagnostics {
    pub files: Vec<FileDiagnostics>,
}

impl WorkspaceDiagnostics {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    #[must_use]
    pub fn total_diagnostics(&self) -> usize {
        self.files.iter().map(|file| file.diagnostics.len()).sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolLocation {
    pub path: PathBuf,
    pub range: Range,
}

impl SymbolLocation {
    #[must_use]
    pub fn start_line(&self) -> u32 {
        self.range.start.line + 1
    }

    #[must_use]
    pub fn start_character(&self) -> u32 {
        self.range.start.character + 1
    }
}

impl Display for SymbolLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.path.display(),
            self.start_line(),
            self.start_character()
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LspContextEnrichment {
    pub file_path: PathBuf,
    pub diagnostics: WorkspaceDiagnostics,
    pub definitions: Vec<SymbolLocation>,
    pub references: Vec<SymbolLocation>,
}

impl LspContextEnrichment {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.diagnostics.is_empty() && self.definitions.is_empty() && self.references.is_empty()
    }

    #[must_use]
    pub fn render_prompt_section(&self) -> String {
        const MAX_RENDERED_DIAGNOSTICS: usize = 12;
        const MAX_RENDERED_LOCATIONS: usize = 12;

        let mut lines = vec!["# LSP context".to_string()];
        lines.push(format!(" - Focus file: {}", self.file_path.display()));
        lines.push(format!(
            " - Workspace diagnostics: {} across {} file(s)",
            self.diagnostics.total_diagnostics(),
            self.diagnostics.files.len()
        ));

        if !self.diagnostics.files.is_empty() {
            lines.push(String::new());
            lines.push("Diagnostics:".to_string());
            let mut rendered = 0usize;
            for file in &self.diagnostics.files {
                for diagnostic in &file.diagnostics {
                    if rendered == MAX_RENDERED_DIAGNOSTICS {
                        lines.push(" - Additional diagnostics omitted for brevity.".to_string());
                        break;
                    }
                    let severity = diagnostic_severity_label(diagnostic.severity);
                    lines.push(format!(
                        " - {}:{}:{} [{}] {}",
                        file.path.display(),
                        diagnostic.range.start.line + 1,
                        diagnostic.range.start.character + 1,
                        severity,
                        diagnostic.message.replace('\n', " ")
                    ));
                    rendered += 1;
                }
                if rendered == MAX_RENDERED_DIAGNOSTICS {
                    break;
                }
            }
        }

        if !self.definitions.is_empty() {
            lines.push(String::new());
            lines.push("Definitions:".to_string());
            lines.extend(
                self.definitions
                    .iter()
                    .take(MAX_RENDERED_LOCATIONS)
                    .map(|location| format!(" - {location}")),
            );
            if self.definitions.len() > MAX_RENDERED_LOCATIONS {
                lines.push(" - Additional definitions omitted for brevity.".to_string());
            }
        }

        if !self.references.is_empty() {
            lines.push(String::new());
            lines.push("References:".to_string());
            lines.extend(
                self.references
                    .iter()
                    .take(MAX_RENDERED_LOCATIONS)
                    .map(|location| format!(" - {location}")),
            );
            if self.references.len() > MAX_RENDERED_LOCATIONS {
                lines.push(" - Additional references omitted for brevity.".to_string());
            }
        }

        lines.join("\n")
    }
}

#[must_use]
pub(crate) fn normalize_extension(extension: &str) -> String {
    if extension.starts_with('.') {
        extension.to_ascii_lowercase()
    } else {
        format!(".{}", extension.to_ascii_lowercase())
    }
}

fn diagnostic_severity_label(severity: Option<lsp_types::DiagnosticSeverity>) -> &'static str {
    match severity {
        Some(lsp_types::DiagnosticSeverity::ERROR) => "error",
        Some(lsp_types::DiagnosticSeverity::WARNING) => "warning",
        Some(lsp_types::DiagnosticSeverity::INFORMATION) => "info",
        Some(lsp_types::DiagnosticSeverity::HINT) => "hint",
        _ => "unknown",
    }
}
