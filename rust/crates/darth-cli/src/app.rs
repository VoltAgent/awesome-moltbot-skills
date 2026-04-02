use std::io::{self, Write};
use std::path::PathBuf;

use crate::args::{OutputFormat, PermissionMode};
use crate::input::{LineEditor, ReadOutcome};
use crate::render::{Spinner, TerminalRenderer};
use darth_runtime::{ConversationClient, ConversationMessage, RuntimeError, StreamEvent, UsageSummary};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    pub model: String,
    pub permission_mode: PermissionMode,
    pub config: Option<PathBuf>,
    pub output_format: OutputFormat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionState {
    pub turns: usize,
    pub compacted_messages: usize,
    pub last_model: String,
    pub last_usage: UsageSummary,
}

impl SessionState {
    #[must_use]
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            turns: 0,
            compacted_messages: 0,
            last_model: model.into(),
            last_usage: UsageSummary::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandResult {
    Continue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlashCommand {
    Help,
    Status,
    Compact,
    Unknown(String),
}

impl SlashCommand {
    #[must_use]
    pub fn parse(input: &str) -> Option<Self> {
        let trimmed = input.trim();
        if !trimmed.starts_with('/') {
            return None;
        }

        let command = trimmed
            .trim_start_matches('/')
            .split_whitespace()
            .next()
            .unwrap_or_default();
        Some(match command {
            "help" => Self::Help,
            "status" => Self::Status,
            "compact" => Self::Compact,
            other => Self::Unknown(other.to_string()),
        })
    }
}

struct SlashCommandHandler {
    command: SlashCommand,
    summary: &'static str,
}

const SLASH_COMMAND_HANDLERS: &[SlashCommandHandler] = &[
    SlashCommandHandler {
        command: SlashCommand::Help,
        summary: "Show command help",
    },
    SlashCommandHandler {
        command: SlashCommand::Status,
        summary: "Show current session status",
    },
    SlashCommandHandler {
        command: SlashCommand::Compact,
        summary: "Compact local session history",
    },
];

pub struct CliApp {
    config: SessionConfig,
    renderer: TerminalRenderer,
    state: SessionState,
    conversation_client: ConversationClient,
    conversation_history: Vec<ConversationMessage>,
}

impl CliApp {
    pub fn new(config: SessionConfig) -> Result<Self, RuntimeError> {
        let state = SessionState::new(config.model.clone());
        let conversation_client = ConversationClient::from_env(config.model.clone())?;
        Ok(Self {
            config,
            renderer: TerminalRenderer::new(),
            state,
            conversation_client,
            conversation_history: Vec::new(),
        })
    }

    pub fn run_repl(&mut self) -> io::Result<()> {
        let mut editor = LineEditor::new("› ", Vec::new());
        println!("Darth Agent interactive mode");
        println!("Type /help for commands. Shift+Enter or Ctrl+J inserts a newline.");

        loop {
            match editor.read_line()? {
                ReadOutcome::Submit(input) => {
                    if input.trim().is_empty() {
                        continue;
                    }
                    self.handle_submission(&input, &mut io::stdout())?;
                }
                ReadOutcome::Cancel => continue,
                ReadOutcome::Exit => break,
            }
        }

        Ok(())
    }

    pub fn run_prompt(&mut self, prompt: &str, out: &mut impl Write) -> io::Result<()> {
        self.render_response(prompt, out)
    }

    pub fn handle_submission(
        &mut self,
        input: &str,
        out: &mut impl Write,
    ) -> io::Result<CommandResult> {
        if let Some(command) = SlashCommand::parse(input) {
            return self.dispatch_slash_command(command, out);
        }

        self.state.turns += 1;
        self.render_response(input, out)?;
        Ok(CommandResult::Continue)
    }

    fn dispatch_slash_command(
        &mut self,
        command: SlashCommand,
        out: &mut impl Write,
    ) -> io::Result<CommandResult> {
        match command {
            SlashCommand::Help => Self::handle_help(out),
            SlashCommand::Status => self.handle_status(out),
            SlashCommand::Compact => self.handle_compact(out),
            SlashCommand::Unknown(name) => {
                writeln!(out, "Unknown slash command: /{name}")?;
                Ok(CommandResult::Continue)
            }
            _ => {
                writeln!(out, "Slash command unavailable in this mode")?;
                Ok(CommandResult::Continue)
            }
        }
    }

    fn handle_help(out: &mut impl Write) -> io::Result<CommandResult> {
        writeln!(out, "Available commands:")?;
        for handler in SLASH_COMMAND_HANDLERS {
            let name = match handler.command {
                SlashCommand::Help => "/help",
                SlashCommand::Status => "/status",
                SlashCommand::Compact => "/compact",
                _ => continue,
            };
            writeln!(out, "  {name:<9} {}", handler.summary)?;
        }
        Ok(CommandResult::Continue)
    }

    fn handle_status(&mut self, out: &mut impl Write) -> io::Result<CommandResult> {
        writeln!(
            out,
            "status: turns={} model={} permission-mode={:?} output-format={:?} last-usage={} in/{} out config={}",
            self.state.turns,
            self.state.last_model,
            self.config.permission_mode,
            self.config.output_format,
            self.state.last_usage.input_tokens,
            self.state.last_usage.output_tokens,
            self.config
                .config
                .as_ref()
                .map_or_else(|| String::from("<none>"), |path| path.display().to_string())
        )?;
        Ok(CommandResult::Continue)
    }

    fn handle_compact(&mut self, out: &mut impl Write) -> io::Result<CommandResult> {
        self.state.compacted_messages += self.state.turns;
        self.state.turns = 0;
        self.conversation_history.clear();
        writeln!(
            out,
            "Compacted session history into a local summary ({} messages total compacted).",
            self.state.compacted_messages
        )?;
        Ok(CommandResult::Continue)
    }

    fn handle_stream_event(
        renderer: &TerminalRenderer,
        event: StreamEvent,
        stream_spinner: &mut Spinner,
        tool_spinner: &mut Spinner,
        saw_text: &mut bool,
        turn_usage: &mut UsageSummary,
        out: &mut impl Write,
    ) {
        match event {
            StreamEvent::TextDelta(delta) => {
                if !*saw_text {
                    let _ =
                        stream_spinner.finish("Streaming response", renderer.color_theme(), out);
                    *saw_text = true;
                }
                let _ = write!(out, "{delta}");
                let _ = out.flush();
            }
            StreamEvent::ToolCallStart { name, input } => {
                if *saw_text {
                    let _ = writeln!(out);
                }
                let _ = tool_spinner.tick(
                    &format!("Running tool `{name}` with {input}"),
                    renderer.color_theme(),
                    out,
                );
            }
            StreamEvent::ToolCallResult {
                name,
                output,
                is_error,
            } => {
                let label = if is_error {
                    format!("Tool `{name}` failed")
                } else {
                    format!("Tool `{name}` completed")
                };
                let _ = tool_spinner.finish(&label, renderer.color_theme(), out);
                let rendered_output = format!("### Tool `{name}`\n\n```text\n{output}\n```\n");
                let _ = renderer.stream_markdown(&rendered_output, out);
            }
            StreamEvent::Usage(usage) => {
                *turn_usage = usage;
            }
        }
    }

    fn write_turn_output(
        &self,
        summary: &darth_runtime::TurnSummary,
        out: &mut impl Write,
    ) -> io::Result<()> {
        match self.config.output_format {
            OutputFormat::Text => {
                writeln!(
                    out,
                    "\nToken usage: {} input / {} output",
                    self.state.last_usage.input_tokens, self.state.last_usage.output_tokens
                )?;
            }
            OutputFormat::Json => {
                writeln!(
                    out,
                    "{}",
                    serde_json::json!({
                        "message": summary.assistant_text,
                        "usage": {
                            "input_tokens": self.state.last_usage.input_tokens,
                            "output_tokens": self.state.last_usage.output_tokens,
                        }
                    })
                )?;
            }
            OutputFormat::Ndjson => {
                writeln!(
                    out,
                    "{}",
                    serde_json::json!({
                        "type": "message",
                        "text": summary.assistant_text,
                        "usage": {
                            "input_tokens": self.state.last_usage.input_tokens,
                            "output_tokens": self.state.last_usage.output_tokens,
                        }
                    })
                )?;
            }
        }
        Ok(())
    }

    fn render_response(&mut self, input: &str, out: &mut impl Write) -> io::Result<()> {
        let mut stream_spinner = Spinner::new();
        stream_spinner.tick(
            "Opening conversation stream",
            self.renderer.color_theme(),
            out,
        )?;

        let mut turn_usage = UsageSummary::default();
        let mut tool_spinner = Spinner::new();
        let mut saw_text = false;
        let renderer = &self.renderer;

        let result =
            self.conversation_client
                .run_turn(&mut self.conversation_history, input, |event| {
                    Self::handle_stream_event(
                        renderer,
                        event,
                        &mut stream_spinner,
                        &mut tool_spinner,
                        &mut saw_text,
                        &mut turn_usage,
                        out,
                    );
                });

        let summary = match result {
            Ok(summary) => summary,
            Err(error) => {
                stream_spinner.fail(
                    "Streaming response failed",
                    self.renderer.color_theme(),
                    out,
                )?;
                return Err(io::Error::other(error));
            }
        };
        self.state.last_usage = summary.usage.clone();
        if saw_text {
            writeln!(out)?;
        } else {
            stream_spinner.finish("Streaming response", self.renderer.color_theme(), out)?;
        }

        self.write_turn_output(&summary, out)?;
        let _ = turn_usage;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::args::{OutputFormat, PermissionMode};

    use super::{CommandResult, SessionConfig, SlashCommand};

    #[test]
    fn parses_required_slash_commands() {
        assert_eq!(SlashCommand::parse("/help"), Some(SlashCommand::Help));
        assert_eq!(SlashCommand::parse(" /status "), Some(SlashCommand::Status));
        assert_eq!(
            SlashCommand::parse("/compact now"),
            Some(SlashCommand::Compact)
        );
    }

    #[test]
    fn help_output_lists_commands() {
        let mut out = Vec::new();
        let result = super::CliApp::handle_help(&mut out).expect("help succeeds");
        assert_eq!(result, CommandResult::Continue);
        let output = String::from_utf8_lossy(&out);
        assert!(output.contains("/help"));
        assert!(output.contains("/status"));
        assert!(output.contains("/compact"));
    }

    #[test]
    fn session_state_tracks_config_values() {
        let config = SessionConfig {
            model: "sonnet".into(),
            permission_mode: PermissionMode::DangerFullAccess,
            config: Some(PathBuf::from("settings.toml")),
            output_format: OutputFormat::Text,
        };

        assert_eq!(config.model, "sonnet");
        assert_eq!(config.permission_mode, PermissionMode::DangerFullAccess);
        assert_eq!(config.config, Some(PathBuf::from("settings.toml")));
    }
}
