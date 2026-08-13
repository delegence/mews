//! WIP interactive terminal UI for MEWS sessions with agents.

use std::{
    collections::HashMap,
    env,
    io::{self, IsTerminal, Write},
    path::Path,
};

use anyhow::Result;
use mews_client::{
    Message, MessageContent, MessageRole, MessageSource, MewsClient, Session, SourceKind,
};
use tokio::task::JoinHandle;

pub async fn chat(client: &mut MewsClient, mut session: Session) -> Result<()> {
    let agent = client
        .agents()
        .await?
        .into_iter()
        .find(|agent| agent.id == session.agent_id)
        .map(|agent| agent.slug)
        .unwrap_or_else(|| session.agent_id.to_string());
    let host = client
        .hosts()
        .await?
        .into_iter()
        .find(|status| status.host.id == session.host_id)
        .map(|status| status.host.name)
        .unwrap_or_else(|| session.host_id.to_string());
    let effective = client.session_model_config(session.id.clone()).await?;
    let model = effective.model.as_deref().unwrap_or("not configured");
    let reasoning = effective.reasoning.map(reasoning_label).unwrap_or("none");
    let colors = Colors::detect();
    render_header(
        &agent,
        &effective.harness,
        model,
        reasoning,
        &session,
        &host,
        &colors,
    );
    render_history(
        &agent,
        client.session_history(session.id.clone()).await?,
        &colors,
    );
    loop {
        print!("> ");
        io::stdout().flush()?;
        let mut line = String::new();
        if io::stdin().read_line(&mut line)? == 0 {
            println!();
            return Ok(());
        }
        let prompt = line.trim();
        if prompt == "/quit" {
            return Ok(());
        } else if let Some(model) = prompt.strip_prefix("/model ") {
            let model = (model != "default").then(|| model.to_owned());
            session = client.set_session_model(session.id.clone(), model).await?;
            println!(
                "model: {}",
                session.model_override.as_deref().unwrap_or("agent default")
            );
        } else if !prompt.is_empty() {
            render_submitted_user_prompt(prompt, &colors)?;
            let answer = send_interactive(client, &session, prompt.to_owned()).await?;
            render_speaker_line(&agent, answer.trim(), &colors);
        }
    }
}

fn render_header(
    agent: &str,
    harness: &str,
    model: &str,
    reasoning: &str,
    session: &Session,
    host: &str,
    colors: &Colors,
) {
    println!(
        "{}------------------------------------{}",
        colors.header, colors.reset
    );
    println!("{}{}{}", colors.agent_title, agent, colors.reset);
    println!(
        "{}------------------------------------{}",
        colors.header, colors.reset
    );
    println!("{}harness: {harness}", colors.header);
    println!("model: {model} ({reasoning})");
    println!("cwd: {} [{host}]", display_path(&session.working_directory));
    println!();
    println!("session: {}", session.id);
    println!("(/quit or Ctrl-D to exit)");
    println!("------------------------------------{}", colors.reset);
}

fn render_history(agent: &str, messages: Vec<Message>, colors: &Colors) {
    for message in messages {
        if let Some((speaker, text)) = history_line(agent, &message.role, &message.content) {
            render_speaker_line(speaker, text, colors);
            continue;
        }
        match message.content {
            MessageContent::ToolCall { tool, .. } => {
                println!("{}◇ calling {tool}{}", colors.activity, colors.reset);
            }
            MessageContent::ToolResult { tool, is_error, .. } => {
                let (marker, color, outcome) = if is_error {
                    ("×", colors.error, "failed")
                } else {
                    ("◇", colors.activity, "completed")
                };
                println!("{color}{marker} {tool} {outcome}{}", colors.reset);
            }
            MessageContent::ProviderState { .. } => {}
            MessageContent::Text { .. } => unreachable!("text entries always render as history"),
        }
    }
}

fn history_line<'a>(
    agent: &'a str,
    role: &MessageRole,
    content: &'a MessageContent,
) -> Option<(&'a str, &'a str)> {
    match content {
        MessageContent::Text { text } => Some(match role {
            MessageRole::User => ("You", text),
            MessageRole::Assistant => (agent, text),
            MessageRole::Tool => ("Tool", text),
        }),
        MessageContent::ToolCall { .. }
        | MessageContent::ToolResult { .. }
        | MessageContent::ProviderState { .. } => None,
    }
}

fn render_speaker_line(speaker: &str, text: &str, colors: &Colors) {
    println!("{}{}:{} {text}", colors.speaker, speaker, colors.reset);
}

fn render_submitted_user_prompt(prompt: &str, colors: &Colors) -> io::Result<()> {
    if io::stdout().is_terminal() {
        print!(
            "\x1b[1A\r\x1b[2K{}You:{} {prompt}\n",
            colors.speaker, colors.reset
        );
    } else {
        render_speaker_line("You", prompt, colors);
    }
    io::stdout().flush()
}

async fn send_interactive(
    client: &mut MewsClient,
    session: &Session,
    prompt: String,
) -> Result<String> {
    let consumer = mews_client::ConsumerId::new();
    let subscribed = client
        .subscribe_as(
            consumer.clone(),
            session.id.clone(),
            mews_client::ConsumerKind::Ephemeral,
        )
        .await;
    if let Err(error) = subscribed {
        let _ = client.delete_consumer(consumer).await;
        return Err(error);
    }
    let result = send_interactive_subscribed(client, session, prompt, consumer.clone()).await;
    let _ = client.delete_consumer(consumer).await;
    result
}

async fn send_interactive_subscribed(
    client: &mut MewsClient,
    session: &Session,
    prompt: String,
    consumer: mews_client::ConsumerId,
) -> Result<String> {
    let turn = client
        .start_turn(
            session.id.clone(),
            prompt,
            serde_json::Value::Null,
            MessageSource {
                kind: SourceKind::Client,
                id: "cli".into(),
                channel_origin: None,
            },
        )
        .await?;
    let mut thinking = ThinkingIndicator::start();
    let colors = Colors::detect();
    let mut reasoning = String::new();
    let mut reasoning_message_id = None;
    let mut reasoning_visible = false;
    let mut pending_tools = HashMap::new();
    loop {
        let batch = match client.poll_events(consumer.clone(), 1_000).await {
            Ok(batch) => batch,
            Err(error) => {
                thinking.stop().await?;
                return Err(error);
            }
        };
        let mut display_events = Vec::new();
        for event in &batch.events {
            match &event.kind {
                mews_client::ClientEventKind::ReasoningDelta {
                    turn_id,
                    delta,
                    message_id,
                } if *turn_id == turn.id => {
                    display_events.push(DisplayEvent::Reasoning {
                        delta: delta.clone(),
                        message_id: message_id.clone(),
                    });
                }
                mews_client::ClientEventKind::ToolActivity { turn_id, activity }
                    if *turn_id == turn.id =>
                {
                    if activity.status.as_deref() == Some("failed") {
                        let description = pending_tools
                            .remove(&activity.call_id)
                            .unwrap_or_else(|| format_acp_activity(activity));
                        display_events.push(DisplayEvent::Trace(Trace::Error(description)));
                    } else if activity.status.as_deref() == Some("completed") {
                        let description = format_acp_activity(activity);
                        let pending = pending_tools.remove(&activity.call_id);
                        let description = if has_activity_detail(activity) {
                            description
                        } else {
                            pending.unwrap_or(description)
                        };
                        display_events.push(DisplayEvent::Trace(Trace::Activity(
                            completed_activity(&description),
                        )));
                    } else {
                        pending_tools
                            .insert(activity.call_id.clone(), format_acp_activity(activity));
                    }
                }
                mews_client::ClientEventKind::ToolStarted { turn_id, message }
                    if *turn_id == turn.id =>
                {
                    if let mews_client::MessageContent::ToolCall {
                        call_id,
                        tool,
                        arguments,
                        ..
                    } = &message.content
                    {
                        pending_tools
                            .insert(call_id.clone(), format_tool_activity(tool, arguments));
                    }
                }
                mews_client::ClientEventKind::ToolCompleted { turn_id, message }
                    if *turn_id == turn.id =>
                {
                    if let mews_client::MessageContent::ToolResult {
                        call_id,
                        tool,
                        is_error: true,
                        ..
                    } = &message.content
                    {
                        display_events.push(DisplayEvent::Trace(Trace::Error(
                            pending_tools
                                .remove(call_id)
                                .unwrap_or_else(|| format!("{tool} failed")),
                        )));
                    } else if let mews_client::MessageContent::ToolResult {
                        call_id,
                        tool,
                        is_error: false,
                        ..
                    } = &message.content
                    {
                        let description = pending_tools
                            .remove(call_id)
                            .unwrap_or_else(|| tool.clone());
                        display_events.push(DisplayEvent::Trace(Trace::Activity(
                            completed_activity(&description),
                        )));
                    }
                }
                _ => {}
            }
        }
        if batch.advanced {
            client
                .acknowledge(consumer.clone(), batch.checkpoint)
                .await?;
        }
        for event in display_events {
            match event {
                DisplayEvent::Reasoning { delta, message_id } => {
                    let message_changed = reasoning_message_id
                        .as_ref()
                        .zip(message_id.as_ref())
                        .is_some_and(|(previous, next)| previous != next);
                    if message_changed {
                        thinking.stop().await?;
                        commit_reasoning(&mut reasoning, &mut reasoning_visible, &colors)?;
                    }
                    if message_id.is_some() {
                        reasoning_message_id = message_id;
                    }
                    reasoning.push_str(&delta);
                    if colors.interactive {
                        thinking.stop().await?;
                        print!(
                            "\r\x1b[2K{}◇ {}{}",
                            colors.reasoning,
                            compact_trace(&reasoning),
                            colors.reset
                        );
                        io::stdout().flush()?;
                        reasoning_visible = true;
                    }
                }
                DisplayEvent::Trace(trace) => {
                    thinking.stop().await?;
                    commit_reasoning(&mut reasoning, &mut reasoning_visible, &colors)?;
                    reasoning_message_id = None;
                    match trace {
                        Trace::Activity(activity) => {
                            println!("{}◇ {activity}{}", colors.activity, colors.reset)
                        }
                        Trace::Error(error) => {
                            println!("{}× {error}{}", colors.error, colors.reset)
                        }
                    }
                    thinking = ThinkingIndicator::start();
                }
            }
        }
        let current = match client.get_turn(turn.id.clone()).await {
            Ok(current) => current,
            Err(error) => {
                thinking.stop().await?;
                return Err(error);
            }
        };
        match client.terminal_turn_answer(&current).await {
            Ok(Some(answer)) => {
                thinking.stop().await?;
                commit_reasoning(&mut reasoning, &mut reasoning_visible, &colors)?;
                return Ok(answer);
            }
            Ok(None) => {}
            Err(error) => {
                thinking.stop().await?;
                commit_reasoning(&mut reasoning, &mut reasoning_visible, &colors)?;
                return Err(error);
            }
        }
    }
}

struct ThinkingIndicator {
    task: Option<JoinHandle<io::Result<()>>>,
}

impl ThinkingIndicator {
    fn start() -> Self {
        if !io::stdout().is_terminal() {
            return Self { task: None };
        }
        Self {
            task: Some(tokio::spawn(async {
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(400));
                let mut dots = 1;
                loop {
                    interval.tick().await;
                    print!("\r{}", ".".repeat(dots));
                    io::stdout().flush()?;
                    dots = dots % 3 + 1;
                }
            })),
        }
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(mut task) = self.task.take() {
            task.abort();
            let _ = (&mut task).await;
            print!("\r\x1b[2K");
            io::stdout().flush()?;
        }
        Ok(())
    }
}

impl Drop for ThinkingIndicator {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct Colors {
    interactive: bool,
    header: &'static str,
    agent_title: &'static str,
    speaker: &'static str,
    activity: &'static str,
    reasoning: &'static str,
    error: &'static str,
    reset: &'static str,
}

enum Trace {
    Activity(String),
    Error(String),
}

enum DisplayEvent {
    Reasoning {
        delta: String,
        message_id: Option<String>,
    },
    Trace(Trace),
}

impl Colors {
    fn detect() -> Self {
        let interactive = io::stdout().is_terminal();
        if interactive && env::var_os("NO_COLOR").is_none() {
            Self {
                interactive: true,
                header: "\x1b[2;37m",
                agent_title: agent_title_color(),
                speaker: "\x1b[1;2;37m",
                activity: "\x1b[2;36m",
                reasoning: "\x1b[2;35m",
                error: "\x1b[31m",
                reset: "\x1b[0m",
            }
        } else {
            Self {
                interactive,
                header: "",
                agent_title: "",
                speaker: "",
                activity: "",
                reasoning: "",
                error: "",
                reset: "",
            }
        }
    }
}

fn agent_title_color() -> &'static str {
    const COLORS: [&str; 6] = [
        "\x1b[1;31m",
        "\x1b[1;32m",
        "\x1b[1;33m",
        "\x1b[1;34m",
        "\x1b[1;35m",
        "\x1b[1;36m",
    ];
    let index = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos() as usize)
        .unwrap_or_default()
        % COLORS.len();
    COLORS[index]
}

fn commit_reasoning(reasoning: &mut String, visible: &mut bool, colors: &Colors) -> Result<()> {
    if *visible {
        println!();
    } else if !reasoning.is_empty() {
        println!(
            "{}◇ {}{}",
            colors.reasoning,
            compact_trace(reasoning),
            colors.reset
        );
    }
    reasoning.clear();
    *visible = false;
    io::stdout().flush()?;
    Ok(())
}

fn compact_trace(text: &str) -> String {
    let compact = text
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace("**", "");
    let mut chars = compact.chars();
    let preview = chars.by_ref().take(160).collect::<String>();
    if chars.next().is_some() {
        format!("{preview}…")
    } else {
        preview
    }
}

fn format_acp_activity(activity: &mews_client::ToolActivity) -> String {
    let kind = activity.kind.as_deref().unwrap_or(&activity.title);
    if matches!(kind, "bash" | "execute") {
        if let Some(command) = acp_command(activity) {
            return format!("running `{}`", compact_trace(&command));
        }
    } else if has_activity_detail(activity) {
        return format_tool_activity(kind, &activity.input);
    }
    activity.title.clone()
}

fn has_activity_detail(activity: &mews_client::ToolActivity) -> bool {
    ["command", "path", "url", "query"].into_iter().any(|key| {
        activity
            .input
            .get(key)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
    })
}

fn acp_command(activity: &mews_client::ToolActivity) -> Option<String> {
    let text_or_array = |value: &serde_json::Value| {
        value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .or_else(|| {
                value.as_array().and_then(|values| {
                    let command = values
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .collect::<Vec<_>>()
                        .join(" ");
                    (!command.is_empty()).then_some(command)
                })
            })
    };
    if let Some(command) = activity.input.get("command").and_then(text_or_array) {
        return Some(command);
    }
    if let Some(executable) = activity
        .input
        .get("executable")
        .and_then(serde_json::Value::as_str)
    {
        let arguments = activity
            .input
            .get("args")
            .and_then(text_or_array)
            .unwrap_or_default();
        return Some(format!("{executable} {arguments}").trim().to_owned());
    }
    activity
        .title
        .split_once('`')
        .and_then(|(_, rest)| rest.split_once('`'))
        .map(|(command, _)| command.to_owned())
        .filter(|command| !command.trim().is_empty())
}

fn format_tool_activity(tool: &str, arguments: &serde_json::Value) -> String {
    let value = |key| {
        arguments
            .get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
    };
    match tool {
        "bash" | "execute" => value("command")
            .map(|command| format!("running `{}`", compact_trace(command)))
            .unwrap_or_else(|| "running command".into()),
        "read" => value("path")
            .map(|path| format!("reading {path}"))
            .unwrap_or_else(|| "reading file".into()),
        "write" => value("path")
            .map(|path| format!("writing {path}"))
            .unwrap_or_else(|| "writing file".into()),
        "edit" => value("path")
            .map(|path| format!("editing {path}"))
            .unwrap_or_else(|| "editing file".into()),
        "search" => value("query")
            .map(|query| format!("searching for `{}`", compact_trace(query)))
            .unwrap_or_else(|| "searching".into()),
        "fetch" => value("url")
            .map(|url| format!("fetching {url}"))
            .unwrap_or_else(|| "fetching resource".into()),
        _ => compact_trace(tool),
    }
}

fn completed_activity(activity: &str) -> String {
    for (ongoing, completed) in [
        ("running ", "ran "),
        ("reading ", "read "),
        ("writing ", "wrote "),
        ("editing ", "edited "),
        ("searching ", "searched "),
        ("fetching ", "fetched "),
    ] {
        if let Some(rest) = activity.strip_prefix(ongoing) {
            return format!("{completed}{rest}");
        }
    }
    activity.to_owned()
}

fn reasoning_label(reasoning: mews_client::ReasoningEffort) -> &'static str {
    match reasoning {
        mews_client::ReasoningEffort::None => "none",
        mews_client::ReasoningEffort::Auto => "auto",
        mews_client::ReasoningEffort::Minimal => "minimal",
        mews_client::ReasoningEffort::Low => "low",
        mews_client::ReasoningEffort::Medium => "medium",
        mews_client::ReasoningEffort::High => "high",
        mews_client::ReasoningEffort::XHigh => "xhigh",
        mews_client::ReasoningEffort::Max => "max",
    }
}

fn display_path(path: &Path) -> String {
    let Some(home) = env::var_os("HOME").map(std::path::PathBuf::from) else {
        return path.display().to_string();
    };
    let Ok(relative) = path.strip_prefix(home) else {
        return path.display().to_string();
    };
    if relative.as_os_str().is_empty() {
        "~".into()
    } else {
        format!("~/{}", relative.display())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        MessageContent, MessageRole, compact_trace, completed_activity, format_acp_activity,
        format_tool_activity, history_line,
    };

    #[test]
    fn history_lines_preserve_conversation_roles() {
        assert_eq!(
            history_line(
                "cope",
                &MessageRole::User,
                &MessageContent::Text {
                    text: "hello".into(),
                },
            ),
            Some(("You", "hello"))
        );
        assert_eq!(
            history_line(
                "cope",
                &MessageRole::Assistant,
                &MessageContent::Text { text: "hi".into() },
            ),
            Some(("cope", "hi"))
        );
    }

    #[test]
    fn formats_common_tool_activity() {
        assert_eq!(
            format_tool_activity("bash", &json!({"command": "cargo test"})),
            "running `cargo test`"
        );
        assert_eq!(
            format_tool_activity("read", &json!({"path": "src/main.rs"})),
            "reading src/main.rs"
        );
        assert_eq!(
            format_tool_activity("fetch", &json!({"url": "https://example.com"})),
            "fetching https://example.com"
        );
    }

    #[test]
    fn compacts_multiline_traces() {
        assert_eq!(
            compact_trace("Checking\n  two   sources"),
            "Checking two sources"
        );
    }

    #[test]
    fn acp_activity_ignores_empty_search_queries() {
        let empty = mews_client::ToolActivity {
            call_id: "search-1".into(),
            title: "Web search".into(),
            kind: Some("search".into()),
            status: Some("in_progress".into()),
            input: json!({"query": ""}),
        };
        let completed = mews_client::ToolActivity {
            status: Some("completed".into()),
            input: json!({"query": "weather in Tashkent"}),
            ..empty.clone()
        };

        assert_eq!(format_acp_activity(&empty), "Web search");
        assert_eq!(
            completed_activity(&format_acp_activity(&completed)),
            "searched for `weather in Tashkent`"
        );
    }

    #[test]
    fn acp_activity_accepts_claude_command_shapes() {
        let activity = |title: &str, input| mews_client::ToolActivity {
            call_id: "command-1".into(),
            title: title.into(),
            kind: Some("execute".into()),
            status: Some("completed".into()),
            input,
        };

        assert_eq!(
            format_acp_activity(&activity(
                "Run command",
                json!({"command": ["git", "status"]})
            )),
            "running `git status`"
        );
        assert_eq!(
            format_acp_activity(&activity(
                "Run command",
                json!({"executable": "cargo", "args": ["test", "-p", "mews-acp"]})
            )),
            "running `cargo test -p mews-acp`"
        );
        assert_eq!(
            format_acp_activity(&activity("Bash `pwd`", json!({}))),
            "running `pwd`"
        );
    }
}
