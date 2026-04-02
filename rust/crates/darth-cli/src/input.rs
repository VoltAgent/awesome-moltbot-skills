use std::borrow::Cow;
use std::io::{self, IsTerminal, Write};

use crossterm::cursor::{MoveToColumn, MoveUp};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::queue;
use crossterm::terminal::{self, Clear, ClearType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOutcome {
    Submit(String),
    Cancel,
    Exit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EditorMode {
    Plain,
    Insert,
    Normal,
    Visual,
    Command,
}

impl EditorMode {
    fn indicator(self, vim_enabled: bool) -> Option<&'static str> {
        if !vim_enabled {
            return None;
        }

        Some(match self {
            Self::Plain => "PLAIN",
            Self::Insert => "INSERT",
            Self::Normal => "NORMAL",
            Self::Visual => "VISUAL",
            Self::Command => "COMMAND",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct YankBuffer {
    text: String,
    linewise: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EditSession {
    text: String,
    cursor: usize,
    mode: EditorMode,
    pending_operator: Option<char>,
    visual_anchor: Option<usize>,
    command_buffer: String,
    command_cursor: usize,
    history_index: Option<usize>,
    history_backup: Option<String>,
    rendered_cursor_row: usize,
    rendered_lines: usize,
}

impl EditSession {
    fn new(vim_enabled: bool) -> Self {
        Self {
            text: String::new(),
            cursor: 0,
            mode: if vim_enabled {
                EditorMode::Insert
            } else {
                EditorMode::Plain
            },
            pending_operator: None,
            visual_anchor: None,
            command_buffer: String::new(),
            command_cursor: 0,
            history_index: None,
            history_backup: None,
            rendered_cursor_row: 0,
            rendered_lines: 1,
        }
    }

    fn active_text(&self) -> &str {
        if self.mode == EditorMode::Command {
            &self.command_buffer
        } else {
            &self.text
        }
    }

    fn current_len(&self) -> usize {
        self.active_text().len()
    }

    fn has_input(&self) -> bool {
        !self.active_text().is_empty()
    }

    fn current_line(&self) -> String {
        self.active_text().to_string()
    }

    fn set_text_from_history(&mut self, entry: String) {
        self.text = entry;
        self.cursor = self.text.len();
        self.pending_operator = None;
        self.visual_anchor = None;
        if self.mode != EditorMode::Plain && self.mode != EditorMode::Insert {
            self.mode = EditorMode::Normal;
        }
    }

    fn enter_insert_mode(&mut self) {
        self.mode = EditorMode::Insert;
        self.pending_operator = None;
        self.visual_anchor = None;
    }

    fn enter_normal_mode(&mut self) {
        self.mode = EditorMode::Normal;
        self.pending_operator = None;
        self.visual_anchor = None;
    }

    fn enter_visual_mode(&mut self) {
        self.mode = EditorMode::Visual;
        self.pending_operator = None;
        self.visual_anchor = Some(self.cursor);
    }

    fn enter_command_mode(&mut self) {
        self.mode = EditorMode::Command;
        self.pending_operator = None;
        self.visual_anchor = None;
        self.command_buffer.clear();
        self.command_buffer.push(':');
        self.command_cursor = self.command_buffer.len();
    }

    fn exit_command_mode(&mut self) {
        self.command_buffer.clear();
        self.command_cursor = 0;
        self.enter_normal_mode();
    }

    fn visible_buffer(&self) -> Cow<'_, str> {
        if self.mode != EditorMode::Visual {
            return Cow::Borrowed(self.active_text());
        }

        let Some(anchor) = self.visual_anchor else {
            return Cow::Borrowed(self.active_text());
        };
        let Some((start, end)) = selection_bounds(&self.text, anchor, self.cursor) else {
            return Cow::Borrowed(self.active_text());
        };

        Cow::Owned(render_selected_text(&self.text, start, end))
    }

    fn prompt<'a>(&self, base_prompt: &'a str, vim_enabled: bool) -> Cow<'a, str> {
        match self.mode.indicator(vim_enabled) {
            Some(mode) => Cow::Owned(format!("[{mode}] {base_prompt}")),
            None => Cow::Borrowed(base_prompt),
        }
    }

    fn clear_render(&self, out: &mut impl Write) -> io::Result<()> {
        if self.rendered_cursor_row > 0 {
            queue!(out, MoveUp(to_u16(self.rendered_cursor_row)?))?;
        }
        queue!(out, MoveToColumn(0), Clear(ClearType::FromCursorDown))?;
        out.flush()
    }

    fn render(
        &mut self,
        out: &mut impl Write,
        base_prompt: &str,
        vim_enabled: bool,
    ) -> io::Result<()> {
        self.clear_render(out)?;

        let prompt = self.prompt(base_prompt, vim_enabled);
        let buffer = self.visible_buffer();
        write!(out, "{prompt}{buffer}")?;

        let (cursor_row, cursor_col, total_lines) = self.cursor_layout(prompt.as_ref());
        let rows_to_move_up = total_lines.saturating_sub(cursor_row + 1);
        if rows_to_move_up > 0 {
            queue!(out, MoveUp(to_u16(rows_to_move_up)?))?;
        }
        queue!(out, MoveToColumn(to_u16(cursor_col)?))?;
        out.flush()?;

        self.rendered_cursor_row = cursor_row;
        self.rendered_lines = total_lines;
        Ok(())
    }

    fn finalize_render(
        &self,
        out: &mut impl Write,
        base_prompt: &str,
        vim_enabled: bool,
    ) -> io::Result<()> {
        self.clear_render(out)?;
        let prompt = self.prompt(base_prompt, vim_enabled);
        let buffer = self.visible_buffer();
        write!(out, "{prompt}{buffer}")?;
        writeln!(out)
    }

    fn cursor_layout(&self, prompt: &str) -> (usize, usize, usize) {
        let active_text = self.active_text();
        let cursor = if self.mode == EditorMode::Command {
            self.command_cursor
        } else {
            self.cursor
        };

        let cursor_prefix = &active_text[..cursor];
        let cursor_row = cursor_prefix.bytes().filter(|byte| *byte == b'\n').count();
        let cursor_col = match cursor_prefix.rsplit_once('\n') {
            Some((_, suffix)) => suffix.chars().count(),
            None => prompt.chars().count() + cursor_prefix.chars().count(),
        };
        let total_lines = active_text.bytes().filter(|byte| *byte == b'\n').count() + 1;
        (cursor_row, cursor_col, total_lines)
    }
}

enum KeyAction {
    Continue,
    Submit(String),
    Cancel,
    Exit,
    ToggleVim,
}

pub struct LineEditor {
    prompt: String,
    completions: Vec<String>,
    history: Vec<String>,
    yank_buffer: YankBuffer,
    vim_enabled: bool,
    completion_state: Option<CompletionState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompletionState {
    prefix: String,
    matches: Vec<String>,
    next_index: usize,
}

impl LineEditor {
    #[must_use]
    pub fn new(prompt: impl Into<String>, completions: Vec<String>) -> Self {
        Self {
            prompt: prompt.into(),
            completions,
            history: Vec::new(),
            yank_buffer: YankBuffer::default(),
            vim_enabled: false,
            completion_state: None,
        }
    }

    pub fn push_history(&mut self, entry: impl Into<String>) {
        let entry = entry.into();
        if entry.trim().is_empty() {
            return;
        }

        self.history.push(entry);
    }

    pub fn read_line(&mut self) -> io::Result<ReadOutcome> {
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            return self.read_line_fallback();
        }

        let _raw_mode = RawModeGuard::new()?;
        let mut stdout = io::stdout();
        let mut session = EditSession::new(self.vim_enabled);
        session.render(&mut stdout, &self.prompt, self.vim_enabled)?;

        loop {
            let Event::Key(key) = event::read()? else {
                continue;
            };
            if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
                continue;
            }

            match self.handle_key_event(&mut session, key) {
                KeyAction::Continue => {
                    session.render(&mut stdout, &self.prompt, self.vim_enabled)?;
                }
                KeyAction::Submit(line) => {
                    session.finalize_render(&mut stdout, &self.prompt, self.vim_enabled)?;
                    return Ok(ReadOutcome::Submit(line));
                }
                KeyAction::Cancel => {
                    session.clear_render(&mut stdout)?;
                    writeln!(stdout)?;
                    return Ok(ReadOutcome::Cancel);
                }
                KeyAction::Exit => {
                    session.clear_render(&mut stdout)?;
                    writeln!(stdout)?;
                    return Ok(ReadOutcome::Exit);
                }
                KeyAction::ToggleVim => {
                    session.clear_render(&mut stdout)?;
                    self.vim_enabled = !self.vim_enabled;
                    writeln!(
                        stdout,
                        "Vim mode {}.",
                        if self.vim_enabled {
                            "enabled"
                        } else {
                            "disabled"
                        }
                    )?;
                    session = EditSession::new(self.vim_enabled);
                    session.render(&mut stdout, &self.prompt, self.vim_enabled)?;
                }
            }
        }
    }

    fn read_line_fallback(&mut self) -> io::Result<ReadOutcome> {
        loop {
            let mut stdout = io::stdout();
            write!(stdout, "{}", self.prompt)?;
            stdout.flush()?;

            let mut buffer = String::new();
            let bytes_read = io::stdin().read_line(&mut buffer)?;
            if bytes_read == 0 {
                return Ok(ReadOutcome::Exit);
            }

            while matches!(buffer.chars().last(), Some('\n' | '\r')) {
                buffer.pop();
            }

            if self.handle_submission(&buffer) == Submission::ToggleVim {
                self.vim_enabled = !self.vim_enabled;
                writeln!(
                    stdout,
                    "Vim mode {}.",
                    if self.vim_enabled {
                        "enabled"
                    } else {
                        "disabled"
                    }
                )?;
                continue;
            }

            return Ok(ReadOutcome::Submit(buffer));
        }
    }

    fn handle_key_event(&mut self, session: &mut EditSession, key: KeyEvent) -> KeyAction {
        if key.code != KeyCode::Tab {
            self.completion_state = None;
        }

        if key.modifiers.contains(KeyModifiers::CONTROL) {
            match key.code {
                KeyCode::Char('c') | KeyCode::Char('C') => {
                    return if session.has_input() {
                        KeyAction::Cancel
                    } else {
                        KeyAction::Exit
                    };
                }
                KeyCode::Char('j') | KeyCode::Char('J') => {
                    if session.mode != EditorMode::Normal && session.mode != EditorMode::Visual {
                        self.insert_active_text(session, "\n");
                    }
                    return KeyAction::Continue;
                }
                KeyCode::Char('d') | KeyCode::Char('D') => {
                    if session.current_len() == 0 {
                        return KeyAction::Exit;
                    }
                    self.delete_char_under_cursor(session);
                    return KeyAction::Continue;
                }
                _ => {}
            }
        }

        match key.code {
            KeyCode::Enter if key.modifiers.contains(KeyModifiers::SHIFT) => {
                if session.mode != EditorMode::Normal && session.mode != EditorMode::Visual {
                    self.insert_active_text(session, "\n");
                }
                KeyAction::Continue
            }
            KeyCode::Enter => self.submit_or_toggle(session),
            KeyCode::Esc => self.handle_escape(session),
            KeyCode::Backspace => {
                self.handle_backspace(session);
                KeyAction::Continue
            }
            KeyCode::Delete => {
                self.delete_char_under_cursor(session);
                KeyAction::Continue
            }
            KeyCode::Left => {
                self.move_left(session);
                KeyAction::Continue
            }
            KeyCode::Right => {
                self.move_right(session);
                KeyAction::Continue
            }
            KeyCode::Up => {
                self.history_up(session);
                KeyAction::Continue
            }
            KeyCode::Down => {
                self.history_down(session);
                KeyAction::Continue
            }
            KeyCode::Home => {
                self.move_line_start(session);
                KeyAction::Continue
            }
            KeyCode::End => {
                self.move_line_end(session);
                KeyAction::Continue
            }
            KeyCode::Tab => {
                self.complete_slash_command(session);
                KeyAction::Continue
            }
            KeyCode::Char(ch) => {
                self.handle_char(session, ch);
                KeyAction::Continue
            }
            _ => KeyAction::Continue,
        }
    }

    fn handle_char(&mut self, session: &mut EditSession, ch: char) {
        match session.mode {
            EditorMode::Plain => self.insert_active_char(session, ch),
            EditorMode::Insert => self.insert_active_char(session, ch),
            EditorMode::Normal => self.handle_normal_char(session, ch),
            EditorMode::Visual => self.handle_visual_char(session, ch),
            EditorMode::Command => self.insert_active_char(session, ch),
        }
    }

    fn handle_normal_char(&mut self, session: &mut EditSession, ch: char) {
        if let Some(operator) = session.pending_operator.take() {
            match (operator, ch) {
                ('d', 'd') => {
                    self.delete_current_line(session);
                    return;
                }
                ('y', 'y') => {
                    self.yank_current_line(session);
                    return;
                }
                _ => {}
            }
        }

        match ch {
            'h' => self.move_left(session),
            'j' => self.move_down(session),
            'k' => self.move_up(session),
            'l' => self.move_right(session),
            'd' | 'y' => session.pending_operator = Some(ch),
            'p' => self.paste_after(session),
            'i' => session.enter_insert_mode(),
            'v' => session.enter_visual_mode(),
            ':' => session.enter_command_mode(),
            _ => {}
        }
    }

    fn handle_visual_char(&mut self, session: &mut EditSession, ch: char) {
        match ch {
            'h' => self.move_left(session),
            'j' => self.move_down(session),
            'k' => self.move_up(session),
            'l' => self.move_right(session),
            'v' => session.enter_normal_mode(),
            _ => {}
        }
    }

    fn handle_escape(&mut self, session: &mut EditSession) -> KeyAction {
        match session.mode {
            EditorMode::Plain => KeyAction::Continue,
            EditorMode::Insert => {
                if session.cursor > 0 {
                    session.cursor = previous_boundary(&session.text, session.cursor);
                }
                session.enter_normal_mode();
                KeyAction::Continue
            }
            EditorMode::Normal => KeyAction::Continue,
            EditorMode::Visual => {
                session.enter_normal_mode();
                KeyAction::Continue
            }
            EditorMode::Command => {
                session.exit_command_mode();
                KeyAction::Continue
            }
        }
    }

    fn handle_backspace(&mut self, session: &mut EditSession) {
        match session.mode {
            EditorMode::Normal | EditorMode::Visual => self.move_left(session),
            EditorMode::Command => {
                if session.command_cursor <= 1 {
                    session.exit_command_mode();
                } else {
                    remove_previous_char(&mut session.command_buffer, &mut session.command_cursor);
                }
            }
            EditorMode::Plain | EditorMode::Insert => {
                remove_previous_char(&mut session.text, &mut session.cursor);
            }
        }
    }

    fn submit_or_toggle(&mut self, session: &EditSession) -> KeyAction {
        let line = session.current_line();
        match self.handle_submission(&line) {
            Submission::Submit => KeyAction::Submit(line),
            Submission::ToggleVim => KeyAction::ToggleVim,
        }
    }

    fn handle_submission(&mut self, line: &str) -> Submission {
        if line.trim() == "/vim" {
            Submission::ToggleVim
        } else {
            Submission::Submit
        }
    }

    fn insert_active_char(&mut self, session: &mut EditSession, ch: char) {
        let mut buffer = [0; 4];
        self.insert_active_text(session, ch.encode_utf8(&mut buffer));
    }

    fn insert_active_text(&mut self, session: &mut EditSession, text: &str) {
        if session.mode == EditorMode::Command {
            session
                .command_buffer
                .insert_str(session.command_cursor, text);
            session.command_cursor += text.len();
        } else {
            session.text.insert_str(session.cursor, text);
            session.cursor += text.len();
        }
    }

    fn move_left(&self, session: &mut EditSession) {
        if session.mode == EditorMode::Command {
            session.command_cursor =
                previous_command_boundary(&session.command_buffer, session.command_cursor);
        } else {
            session.cursor = previous_boundary(&session.text, session.cursor);
        }
    }

    fn move_right(&self, session: &mut EditSession) {
        if session.mode == EditorMode::Command {
            session.command_cursor = next_boundary(&session.command_buffer, session.command_cursor);
        } else {
            session.cursor = next_boundary(&session.text, session.cursor);
        }
    }

    fn move_line_start(&self, session: &mut EditSession) {
        if session.mode == EditorMode::Command {
            session.command_cursor = 1;
        } else {
            session.cursor = line_start(&session.text, session.cursor);
        }
    }

    fn move_line_end(&self, session: &mut EditSession) {
        if session.mode == EditorMode::Command {
            session.command_cursor = session.command_buffer.len();
        } else {
            session.cursor = line_end(&session.text, session.cursor);
        }
    }

    fn move_up(&self, session: &mut EditSession) {
        if session.mode == EditorMode::Command {
            return;
        }
        session.cursor = move_vertical(&session.text, session.cursor, -1);
    }

    fn move_down(&self, session: &mut EditSession) {
        if session.mode == EditorMode::Command {
            return;
        }
        session.cursor = move_vertical(&session.text, session.cursor, 1);
    }

    fn delete_char_under_cursor(&self, session: &mut EditSession) {
        match session.mode {
            EditorMode::Command => {
                if session.command_cursor < session.command_buffer.len() {
                    let end = next_boundary(&session.command_buffer, session.command_cursor);
                    session.command_buffer.drain(session.command_cursor..end);
                }
            }
            _ => {
                if session.cursor < session.text.len() {
                    let end = next_boundary(&session.text, session.cursor);
                    session.text.drain(session.cursor..end);
                }
            }
        }
    }

    fn delete_current_line(&mut self, session: &mut EditSession) {
        let (line_start_idx, line_end_idx, delete_start_idx) =
            current_line_delete_range(&session.text, session.cursor);
        self.yank_buffer.text = session.text[line_start_idx..line_end_idx].to_string();
        self.yank_buffer.linewise = true;
        session.text.drain(delete_start_idx..line_end_idx);
        session.cursor = delete_start_idx.min(session.text.len());
    }

    fn yank_current_line(&mut self, session: &mut EditSession) {
        let (line_start_idx, line_end_idx, _) =
            current_line_delete_range(&session.text, session.cursor);
        self.yank_buffer.text = session.text[line_start_idx..line_end_idx].to_string();
        self.yank_buffer.linewise = true;
    }

    fn paste_after(&mut self, session: &mut EditSession) {
        if self.yank_buffer.text.is_empty() {
            return;
        }

        if self.yank_buffer.linewise {
            let line_end_idx = line_end(&session.text, session.cursor);
            let insert_at = if line_end_idx < session.text.len() {
                line_end_idx + 1
            } else {
                session.text.len()
            };
            let mut insertion = self.yank_buffer.text.clone();
            if insert_at == session.text.len()
                && !session.text.is_empty()
                && !session.text.ends_with('\n')
            {
                insertion.insert(0, '\n');
            }
            if insert_at < session.text.len() && !insertion.ends_with('\n') {
                insertion.push('\n');
            }
            session.text.insert_str(insert_at, &insertion);
            session.cursor = if insertion.starts_with('\n') {
                insert_at + 1
            } else {
                insert_at
            };
            return;
        }

        let insert_at = next_boundary(&session.text, session.cursor);
        session.text.insert_str(insert_at, &self.yank_buffer.text);
        session.cursor = insert_at + self.yank_buffer.text.len();
    }

    fn complete_slash_command(&mut self, session: &mut EditSession) {
        if session.mode == EditorMode::Command {
            self.completion_state = None;
            return;
        }
        if let Some(state) = self
            .completion_state
            .as_mut()
            .filter(|_| session.cursor == session.text.len())
            .filter(|state| {
                state
                    .matches
                    .iter()
                    .any(|candidate| candidate == &session.text)
            })
        {
            let candidate = state.matches[state.next_index % state.matches.len()].clone();
            state.next_index += 1;
            session.text.replace_range(..session.cursor, &candidate);
            session.cursor = candidate.len();
            return;
        }
        let Some(prefix) = slash_command_prefix(&session.text, session.cursor) else {
            self.completion_state = None;
            return;
        };
        let matches = self
            .completions
            .iter()
            .filter(|candidate| candidate.starts_with(prefix) && candidate.as_str() != prefix)
            .cloned()
            .collect::<Vec<_>>();
        if matches.is_empty() {
            self.completion_state = None;
            return;
        }

        let candidate = if let Some(state) = self
            .completion_state
            .as_mut()
            .filter(|state| state.prefix == prefix && state.matches == matches)
        {
            let index = state.next_index % state.matches.len();
            state.next_index += 1;
            state.matches[index].clone()
        } else {
            let candidate = matches[0].clone();
            self.completion_state = Some(CompletionState {
                prefix: prefix.to_string(),
                matches,
                next_index: 1,
            });
            candidate
        };

        session.text.replace_range(..session.cursor, &candidate);
        session.cursor = candidate.len();
    }

    fn history_up(&self, session: &mut EditSession) {
        if session.mode == EditorMode::Command || self.history.is_empty() {
            return;
        }

        let next_index = match session.history_index {
            Some(index) => index.saturating_sub(1),
            None => {
                session.history_backup = Some(session.text.clone());
                self.history.len() - 1
            }
        };

        session.history_index = Some(next_index);
        session.set_text_from_history(self.history[next_index].clone());
    }

    fn history_down(&self, session: &mut EditSession) {
        if session.mode == EditorMode::Command {
            return;
        }

        let Some(index) = session.history_index else {
            return;
        };

        if index + 1 < self.history.len() {
            let next_index = index + 1;
            session.history_index = Some(next_index);
            session.set_text_from_history(self.history[next_index].clone());
            return;
        }

        session.history_index = None;
        let restored = session.history_backup.take().unwrap_or_default();
        session.set_text_from_history(restored);
        if self.vim_enabled {
            session.enter_insert_mode();
        } else {
            session.mode = EditorMode::Plain;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Submission {
    Submit,
    ToggleVim,
}

struct RawModeGuard;

impl RawModeGuard {
    fn new() -> io::Result<Self> {
        terminal::enable_raw_mode().map_err(io::Error::other)?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = terminal::disable_raw_mode();
    }
}

fn previous_boundary(text: &str, cursor: usize) -> usize {
    if cursor == 0 {
        return 0;
    }

    text[..cursor]
        .char_indices()
        .next_back()
        .map_or(0, |(index, _)| index)
}

fn previous_command_boundary(text: &str, cursor: usize) -> usize {
    previous_boundary(text, cursor).max(1)
}

fn next_boundary(text: &str, cursor: usize) -> usize {
    if cursor >= text.len() {
        return text.len();
    }

    text[cursor..]
        .chars()
        .next()
        .map_or(text.len(), |ch| cursor + ch.len_utf8())
}

fn remove_previous_char(text: &mut String, cursor: &mut usize) {
    if *cursor == 0 {
        return;
    }

    let start = previous_boundary(text, *cursor);
    text.drain(start..*cursor);
    *cursor = start;
}

fn line_start(text: &str, cursor: usize) -> usize {
    text[..cursor].rfind('\n').map_or(0, |index| index + 1)
}

fn line_end(text: &str, cursor: usize) -> usize {
    text[cursor..]
        .find('\n')
        .map_or(text.len(), |index| cursor + index)
}

fn move_vertical(text: &str, cursor: usize, delta: isize) -> usize {
    let starts = line_starts(text);
    let current_row = text[..cursor].bytes().filter(|byte| *byte == b'\n').count();
    let current_start = starts[current_row];
    let current_col = text[current_start..cursor].chars().count();

    let max_row = starts.len().saturating_sub(1) as isize;
    let target_row = (current_row as isize + delta).clamp(0, max_row) as usize;
    if target_row == current_row {
        return cursor;
    }

    let target_start = starts[target_row];
    let target_end = if target_row + 1 < starts.len() {
        starts[target_row + 1] - 1
    } else {
        text.len()
    };
    byte_index_for_char_column(&text[target_start..target_end], current_col) + target_start
}

fn line_starts(text: &str) -> Vec<usize> {
    let mut starts = vec![0];
    for (index, ch) in text.char_indices() {
        if ch == '\n' {
            starts.push(index + 1);
        }
    }
    starts
}

fn byte_index_for_char_column(text: &str, column: usize) -> usize {
    let mut current = 0;
    for (index, _) in text.char_indices() {
        if current == column {
            return index;
        }
        current += 1;
    }
    text.len()
}

fn current_line_delete_range(text: &str, cursor: usize) -> (usize, usize, usize) {
    let line_start_idx = line_start(text, cursor);
    let line_end_core = line_end(text, cursor);
    let line_end_idx = if line_end_core < text.len() {
        line_end_core + 1
    } else {
        line_end_core
    };
    let delete_start_idx = if line_end_idx == text.len() && line_start_idx > 0 {
        line_start_idx - 1
    } else {
        line_start_idx
    };
    (line_start_idx, line_end_idx, delete_start_idx)
}

fn selection_bounds(text: &str, anchor: usize, cursor: usize) -> Option<(usize, usize)> {
    if text.is_empty() {
        return None;
    }

    if cursor >= anchor {
        let end = next_boundary(text, cursor);
        Some((anchor.min(text.len()), end.min(text.len())))
    } else {
        let end = next_boundary(text, anchor);
        Some((cursor.min(text.len()), end.min(text.len())))
    }
}

fn render_selected_text(text: &str, start: usize, end: usize) -> String {
    let mut rendered = String::new();
    let mut in_selection = false;

    for (index, ch) in text.char_indices() {
        if !in_selection && index == start {
            rendered.push_str("\x1b[7m");
            in_selection = true;
        }
        if in_selection && index == end {
            rendered.push_str("\x1b[0m");
            in_selection = false;
        }
        rendered.push(ch);
    }

    if in_selection {
        rendered.push_str("\x1b[0m");
    }

    rendered
}

fn slash_command_prefix(line: &str, pos: usize) -> Option<&str> {
    if pos != line.len() {
        return None;
    }

    let prefix = &line[..pos];
    if prefix.contains(char::is_whitespace) || !prefix.starts_with('/') {
        return None;
    }

    Some(prefix)
}

fn to_u16(value: usize) -> io::Result<u16> {
    u16::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "terminal position overflowed u16",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{
        selection_bounds, slash_command_prefix, EditSession, EditorMode, KeyAction, LineEditor,
    };
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    #[test]
    fn extracts_only_terminal_slash_command_prefixes() {
        // given
        let complete_prefix = slash_command_prefix("/he", 3);
        let whitespace_prefix = slash_command_prefix("/help me", 5);
        let plain_text_prefix = slash_command_prefix("hello", 5);
        let mid_buffer_prefix = slash_command_prefix("/help", 2);

        // when
        let result = (
            complete_prefix,
            whitespace_prefix,
            plain_text_prefix,
            mid_buffer_prefix,
        );

        // then
        assert_eq!(result, (Some("/he"), None, None, None));
    }

    #[test]
    fn toggle_submission_flips_vim_mode() {
        // given
        let mut editor = LineEditor::new("> ", vec!["/help".to_string(), "/vim".to_string()]);

        // when
        let first = editor.handle_submission("/vim");
        editor.vim_enabled = true;
        let second = editor.handle_submission("/vim");

        // then
        assert!(matches!(first, super::Submission::ToggleVim));
        assert!(matches!(second, super::Submission::ToggleVim));
    }

    #[test]
    fn normal_mode_supports_motion_and_insert_transition() {
        // given
        let mut editor = LineEditor::new("> ", vec![]);
        editor.vim_enabled = true;
        let mut session = EditSession::new(true);
        session.text = "hello".to_string();
        session.cursor = session.text.len();
        let _ = editor.handle_escape(&mut session);

        // when
        editor.handle_char(&mut session, 'h');
        editor.handle_char(&mut session, 'i');
        editor.handle_char(&mut session, '!');

        // then
        assert_eq!(session.mode, EditorMode::Insert);
        assert_eq!(session.text, "hel!lo");
    }

    #[test]
    fn yy_and_p_paste_yanked_line_after_current_line() {
        // given
        let mut editor = LineEditor::new("> ", vec![]);
        editor.vim_enabled = true;
        let mut session = EditSession::new(true);
        session.text = "alpha\nbeta\ngamma".to_string();
        session.cursor = 0;
        let _ = editor.handle_escape(&mut session);

        // when
        editor.handle_char(&mut session, 'y');
        editor.handle_char(&mut session, 'y');
        editor.handle_char(&mut session, 'p');

        // then
        assert_eq!(session.text, "alpha\nalpha\nbeta\ngamma");
    }

    #[test]
    fn dd_and_p_paste_deleted_line_after_current_line() {
        // given
        let mut editor = LineEditor::new("> ", vec![]);
        editor.vim_enabled = true;
        let mut session = EditSession::new(true);
        session.text = "alpha\nbeta\ngamma".to_string();
        session.cursor = 0;
        let _ = editor.handle_escape(&mut session);

        // when
        editor.handle_char(&mut session, 'j');
        editor.handle_char(&mut session, 'd');
        editor.handle_char(&mut session, 'd');
        editor.handle_char(&mut session, 'p');

        // then
        assert_eq!(session.text, "alpha\ngamma\nbeta\n");
    }

    #[test]
    fn visual_mode_tracks_selection_with_motions() {
        // given
        let mut editor = LineEditor::new("> ", vec![]);
        editor.vim_enabled = true;
        let mut session = EditSession::new(true);
        session.text = "alpha\nbeta".to_string();
        session.cursor = 0;
        let _ = editor.handle_escape(&mut session);

        // when
        editor.handle_char(&mut session, 'v');
        editor.handle_char(&mut session, 'j');
        editor.handle_char(&mut session, 'l');

        // then
        assert_eq!(session.mode, EditorMode::Visual);
        assert_eq!(
            selection_bounds(
                &session.text,
                session.visual_anchor.unwrap_or(0),
                session.cursor
            ),
            Some((0, 8))
        );
    }

    #[test]
    fn command_mode_submits_colon_prefixed_input() {
        // given
        let mut editor = LineEditor::new("> ", vec![]);
        editor.vim_enabled = true;
        let mut session = EditSession::new(true);
        session.text = "draft".to_string();
        session.cursor = session.text.len();
        let _ = editor.handle_escape(&mut session);

        // when
        editor.handle_char(&mut session, ':');
        editor.handle_char(&mut session, 'q');
        editor.handle_char(&mut session, '!');
        let action = editor.submit_or_toggle(&session);

        // then
        assert_eq!(session.mode, EditorMode::Command);
        assert_eq!(session.command_buffer, ":q!");
        assert!(matches!(action, KeyAction::Submit(line) if line == ":q!"));
    }

    #[test]
    fn push_history_ignores_blank_entries() {
        // given
        let mut editor = LineEditor::new("> ", vec!["/help".to_string()]);

        // when
        editor.push_history("   ");
        editor.push_history("/help");

        // then
        assert_eq!(editor.history, vec!["/help".to_string()]);
    }

    #[test]
    fn tab_completes_matching_slash_commands() {
        // given
        let mut editor = LineEditor::new("> ", vec!["/help".to_string(), "/hello".to_string()]);
        let mut session = EditSession::new(false);
        session.text = "/he".to_string();
        session.cursor = session.text.len();

        // when
        editor.complete_slash_command(&mut session);

        // then
        assert_eq!(session.text, "/help");
        assert_eq!(session.cursor, 5);
    }

    #[test]
    fn tab_cycles_between_matching_slash_commands() {
        // given
        let mut editor = LineEditor::new(
            "> ",
            vec!["/permissions".to_string(), "/plugin".to_string()],
        );
        let mut session = EditSession::new(false);
        session.text = "/p".to_string();
        session.cursor = session.text.len();

        // when
        editor.complete_slash_command(&mut session);
        let first = session.text.clone();
        session.cursor = session.text.len();
        editor.complete_slash_command(&mut session);
        let second = session.text.clone();

        // then
        assert_eq!(first, "/permissions");
        assert_eq!(second, "/plugin");
    }

    #[test]
    fn ctrl_c_cancels_when_input_exists() {
        // given
        let mut editor = LineEditor::new("> ", vec![]);
        let mut session = EditSession::new(false);
        session.text = "draft".to_string();
        session.cursor = session.text.len();

        // when
        let action = editor.handle_key_event(
            &mut session,
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );

        // then
        assert!(matches!(action, KeyAction::Cancel));
    }
}
