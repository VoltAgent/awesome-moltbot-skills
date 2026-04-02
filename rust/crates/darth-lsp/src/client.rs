use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use lsp_types::{
    Diagnostic, GotoDefinitionResponse, Location, LocationLink, Position, PublishDiagnosticsParams,
};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{oneshot, Mutex};

use crate::error::LspError;
use crate::types::{LspServerConfig, SymbolLocation};

pub(crate) struct LspClient {
    config: LspServerConfig,
    writer: Mutex<BufWriter<ChildStdin>>,
    child: Mutex<Child>,
    pending_requests: Arc<Mutex<BTreeMap<i64, oneshot::Sender<Result<Value, LspError>>>>>,
    diagnostics: Arc<Mutex<BTreeMap<String, Vec<Diagnostic>>>>,
    open_documents: Mutex<BTreeMap<PathBuf, i32>>,
    next_request_id: AtomicI64,
}

impl LspClient {
    pub(crate) async fn connect(config: LspServerConfig) -> Result<Self, LspError> {
        let mut command = Command::new(&config.command);
        command
            .args(&config.args)
            .current_dir(&config.workspace_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .envs(config.env.clone());

        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| LspError::Protocol("missing LSP stdin pipe".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| LspError::Protocol("missing LSP stdout pipe".to_string()))?;
        let stderr = child.stderr.take();

        let client = Self {
            config,
            writer: Mutex::new(BufWriter::new(stdin)),
            child: Mutex::new(child),
            pending_requests: Arc::new(Mutex::new(BTreeMap::new())),
            diagnostics: Arc::new(Mutex::new(BTreeMap::new())),
            open_documents: Mutex::new(BTreeMap::new()),
            next_request_id: AtomicI64::new(1),
        };

        client.spawn_reader(stdout);
        if let Some(stderr) = stderr {
            client.spawn_stderr_drain(stderr);
        }
        client.initialize().await?;
        Ok(client)
    }

    pub(crate) async fn ensure_document_open(&self, path: &Path) -> Result<(), LspError> {
        if self.is_document_open(path).await {
            return Ok(());
        }

        let contents = std::fs::read_to_string(path)?;
        self.open_document(path, &contents).await
    }

    pub(crate) async fn open_document(&self, path: &Path, text: &str) -> Result<(), LspError> {
        let uri = file_url(path)?;
        let language_id = self
            .config
            .language_id_for(path)
            .ok_or_else(|| LspError::UnsupportedDocument(path.to_path_buf()))?;

        self.notify(
            "textDocument/didOpen",
            json!({
                "textDocument": {
                    "uri": uri,
                    "languageId": language_id,
                    "version": 1,
                    "text": text,
                }
            }),
        )
        .await?;

        self.open_documents
            .lock()
            .await
            .insert(path.to_path_buf(), 1);
        Ok(())
    }

    pub(crate) async fn change_document(&self, path: &Path, text: &str) -> Result<(), LspError> {
        if !self.is_document_open(path).await {
            return self.open_document(path, text).await;
        }

        let uri = file_url(path)?;
        let next_version = {
            let mut open_documents = self.open_documents.lock().await;
            let version = open_documents
                .entry(path.to_path_buf())
                .and_modify(|value| *value += 1)
                .or_insert(1);
            *version
        };

        self.notify(
            "textDocument/didChange",
            json!({
                "textDocument": {
                    "uri": uri,
                    "version": next_version,
                },
                "contentChanges": [{
                    "text": text,
                }],
            }),
        )
        .await
    }

    pub(crate) async fn save_document(&self, path: &Path) -> Result<(), LspError> {
        if !self.is_document_open(path).await {
            return Ok(());
        }

        self.notify(
            "textDocument/didSave",
            json!({
                "textDocument": {
                    "uri": file_url(path)?,
                }
            }),
        )
        .await
    }

    pub(crate) async fn close_document(&self, path: &Path) -> Result<(), LspError> {
        if !self.is_document_open(path).await {
            return Ok(());
        }

        self.notify(
            "textDocument/didClose",
            json!({
                "textDocument": {
                    "uri": file_url(path)?,
                }
            }),
        )
        .await?;

        self.open_documents.lock().await.remove(path);
        Ok(())
    }

    pub(crate) async fn is_document_open(&self, path: &Path) -> bool {
        self.open_documents.lock().await.contains_key(path)
    }

    pub(crate) async fn go_to_definition(
        &self,
        path: &Path,
        position: Position,
    ) -> Result<Vec<SymbolLocation>, LspError> {
        self.ensure_document_open(path).await?;
        let response = self
            .request::<Option<GotoDefinitionResponse>>(
                "textDocument/definition",
                json!({
                    "textDocument": { "uri": file_url(path)? },
                    "position": position,
                }),
            )
            .await?;

        Ok(match response {
            Some(GotoDefinitionResponse::Scalar(location)) => {
                location_to_symbol_locations(vec![location])
            }
            Some(GotoDefinitionResponse::Array(locations)) => location_to_symbol_locations(locations),
            Some(GotoDefinitionResponse::Link(links)) => location_links_to_symbol_locations(links),
            None => Vec::new(),
        })
    }

    pub(crate) async fn find_references(
        &self,
        path: &Path,
        position: Position,
        include_declaration: bool,
    ) -> Result<Vec<SymbolLocation>, LspError> {
        self.ensure_document_open(path).await?;
        let response = self
            .request::<Option<Vec<Location>>>(
                "textDocument/references",
                json!({
                    "textDocument": { "uri": file_url(path)? },
                    "position": position,
                    "context": {
                        "includeDeclaration": include_declaration,
                    },
                }),
            )
            .await?;

        Ok(location_to_symbol_locations(response.unwrap_or_default()))
    }

    pub(crate) async fn diagnostics_snapshot(&self) -> BTreeMap<String, Vec<Diagnostic>> {
        self.diagnostics.lock().await.clone()
    }

    pub(crate) async fn shutdown(&self) -> Result<(), LspError> {
        let _ = self.request::<Value>("shutdown", json!({})).await;
        let _ = self.notify("exit", Value::Null).await;

        let mut child = self.child.lock().await;
        if child.kill().await.is_err() {
            let _ = child.wait().await;
            return Ok(());
        }
        let _ = child.wait().await;
        Ok(())
    }

    fn spawn_reader(&self, stdout: ChildStdout) {
        let diagnostics = &self.diagnostics;
        let pending_requests = &self.pending_requests;

        let diagnostics = diagnostics.clone();
        let pending_requests = pending_requests.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            let result = async {
                while let Some(message) = read_message(&mut reader).await? {
                    if let Some(id) = message.get("id").and_then(Value::as_i64) {
                        let response = if let Some(error) = message.get("error") {
                            Err(LspError::Protocol(error.to_string()))
                        } else {
                            Ok(message.get("result").cloned().unwrap_or(Value::Null))
                        };

                        if let Some(sender) = pending_requests.lock().await.remove(&id) {
                            let _ = sender.send(response);
                        }
                        continue;
                    }

                    let Some(method) = message.get("method").and_then(Value::as_str) else {
                        continue;
                    };
                    if method != "textDocument/publishDiagnostics" {
                        continue;
                    }

                    let params = message.get("params").cloned().unwrap_or(Value::Null);
                    let notification = serde_json::from_value::<PublishDiagnosticsParams>(params)?;
                    let mut diagnostics_map = diagnostics.lock().await;
                    if notification.diagnostics.is_empty() {
                        diagnostics_map.remove(&notification.uri.to_string());
                    } else {
                        diagnostics_map.insert(notification.uri.to_string(), notification.diagnostics);
                    }
                }
                Ok::<(), LspError>(())
            }
            .await;

            if let Err(error) = result {
                let mut pending = pending_requests.lock().await;
                let drained = pending
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>();
                for id in drained {
                    if let Some(sender) = pending.remove(&id) {
                        let _ = sender.send(Err(LspError::Protocol(error.to_string())));
                    }
                }
            }
        });
    }

    fn spawn_stderr_drain<R>(&self, stderr: R)
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut sink = Vec::new();
            let _ = reader.read_to_end(&mut sink).await;
        });
    }

    async fn initialize(&self) -> Result<(), LspError> {
        let workspace_uri = file_url(&self.config.workspace_root)?;
        let _ = self
            .request::<Value>(
                "initialize",
                json!({
                    "processId": std::process::id(),
                    "rootUri": workspace_uri,
                    "rootPath": self.config.workspace_root,
                    "workspaceFolders": [{
                        "uri": workspace_uri,
                        "name": self.config.name,
                    }],
                    "initializationOptions": self.config.initialization_options.clone().unwrap_or(Value::Null),
                    "capabilities": {
                        "textDocument": {
                            "publishDiagnostics": {
                                "relatedInformation": true,
                            },
                            "definition": {
                                "linkSupport": true,
                            },
                            "references": {}
                        },
                        "workspace": {
                            "configuration": false,
                            "workspaceFolders": true,
                        },
                        "general": {
                            "positionEncodings": ["utf-16"],
                        }
                    }
                }),
            )
            .await?;
        self.notify("initialized", json!({})).await
    }

    async fn request<T>(&self, method: &str, params: Value) -> Result<T, LspError>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending_requests.lock().await.insert(id, sender);

        if let Err(error) = self
            .send_message(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }))
            .await
        {
            self.pending_requests.lock().await.remove(&id);
            return Err(error);
        }

        let response = receiver
            .await
            .map_err(|_| LspError::Protocol(format!("request channel closed for {method}")))??;
        Ok(serde_json::from_value(response)?)
    }

    async fn notify(&self, method: &str, params: Value) -> Result<(), LspError> {
        self.send_message(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }))
        .await
    }

    async fn send_message(&self, payload: &Value) -> Result<(), LspError> {
        let body = serde_json::to_vec(payload)?;
        let mut writer = self.writer.lock().await;
        writer
            .write_all(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes())
            .await?;
        writer.write_all(&body).await?;
        writer.flush().await?;
        Ok(())
    }
}

async fn read_message<R>(reader: &mut BufReader<R>) -> Result<Option<Value>, LspError>
where
    R: AsyncRead + Unpin,
{
    let mut content_length = None;

    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line).await?;
        if read == 0 {
            return Ok(None);
        }

        if line == "\r\n" {
            break;
        }

        let trimmed = line.trim_end_matches(['\r', '\n']);
        if let Some((name, value)) = trimmed.split_once(':') {
            if name.eq_ignore_ascii_case("Content-Length") {
                let value = value.trim().to_string();
                content_length = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| LspError::InvalidContentLength(value.clone()))?,
                );
            }
        } else {
            return Err(LspError::InvalidHeader(trimmed.to_string()));
        }
    }

    let content_length = content_length.ok_or(LspError::MissingContentLength)?;
    let mut body = vec![0_u8; content_length];
    reader.read_exact(&mut body).await?;
    Ok(Some(serde_json::from_slice(&body)?))
}

fn file_url(path: &Path) -> Result<String, LspError> {
    url::Url::from_file_path(path)
        .map(|url| url.to_string())
        .map_err(|()| LspError::PathToUrl(path.to_path_buf()))
}

fn location_to_symbol_locations(locations: Vec<Location>) -> Vec<SymbolLocation> {
    locations
        .into_iter()
        .filter_map(|location| {
            uri_to_path(&location.uri.to_string()).map(|path| SymbolLocation {
                path,
                range: location.range,
            })
        })
        .collect()
}

fn location_links_to_symbol_locations(links: Vec<LocationLink>) -> Vec<SymbolLocation> {
    links.into_iter()
        .filter_map(|link| {
            uri_to_path(&link.target_uri.to_string()).map(|path| SymbolLocation {
                path,
                range: link.target_selection_range,
            })
        })
        .collect()
}

fn uri_to_path(uri: &str) -> Option<PathBuf> {
    url::Url::parse(uri).ok()?.to_file_path().ok()
}
