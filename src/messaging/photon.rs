//! Photon iMessage adapter backed by a Bun sidecar over JSON lines.

use crate::messaging::apply_runtime_adapter_to_conversation_id;
use crate::messaging::traits::{HistoryMessage, InboundStream, Messaging};
use crate::{
    Attachment, InboundMessage, MessageContent, OutboundResponse, StatusUpdate, metadata_keys,
};

use anyhow::Context as _;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::process::Command;
use tokio::sync::{RwLock, mpsc, oneshot};

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(20);
const INITIAL_RECONNECT_BACKOFF: Duration = Duration::from_secs(2);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);

const PHOTON_SOURCE: &str = "photon";

/// Photon adapter state.
pub struct PhotonAdapter {
    runtime_key: String,
    project_id: String,
    project_secret: String,
    sidecar_command: Option<String>,
    sidecar_working_dir: Option<PathBuf>,
    dm_allowed_users: HashSet<String>,
    stream_buffers: Arc<RwLock<HashMap<String, String>>>,
    command_tx: Arc<RwLock<Option<mpsc::Sender<SidecarCommandRequest>>>>,
    shutdown_tx: Arc<RwLock<Option<mpsc::Sender<()>>>>,
}

impl PhotonAdapter {
    pub fn new(
        runtime_key: impl Into<String>,
        project_id: impl Into<String>,
        project_secret: impl Into<String>,
        sidecar_command: Option<String>,
        sidecar_working_dir: Option<PathBuf>,
        dm_allowed_users: Vec<String>,
    ) -> Self {
        let dm_allowed_users = dm_allowed_users
            .into_iter()
            .map(|entry| entry.trim().to_string())
            .filter(|entry| !entry.is_empty())
            .collect::<HashSet<_>>();

        Self {
            runtime_key: runtime_key.into(),
            project_id: project_id.into(),
            project_secret: project_secret.into(),
            sidecar_command,
            sidecar_working_dir,
            dm_allowed_users,
            stream_buffers: Arc::new(RwLock::new(HashMap::new())),
            command_tx: Arc::new(RwLock::new(None)),
            shutdown_tx: Arc::new(RwLock::new(None)),
        }
    }

    fn extract_space_id(&self, message: &InboundMessage) -> anyhow::Result<String> {
        message
            .metadata
            .get("photon_space_id")
            .and_then(|value| value.as_str())
            .map(ToOwned::to_owned)
            .or_else(|| {
                message
                    .conversation_id
                    .split(':')
                    .next_back()
                    .map(ToOwned::to_owned)
            })
            .filter(|value| !value.trim().is_empty())
            .context("missing photon_space_id in metadata")
    }

    fn extract_message_id(&self, message: &InboundMessage) -> Option<String> {
        message
            .metadata
            .get("photon_message_id")
            .and_then(|value| value.as_str())
            .map(ToOwned::to_owned)
            .filter(|value| !value.trim().is_empty())
    }

    /// True when Photon marked this inbound as referencing the bot — inline reply thread or explicit mention policy.
    fn inbound_expects_inline_reply_thread(&self, message: &InboundMessage) -> bool {
        message
            .metadata
            .get("photon_mentions_or_replies_to_bot")
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
    }

    /// Send chat text via the sidecar: threaded only when explicitly requested (`ThreadReply`)
    /// or Photon flags that the inbound message replies to / mentions the bot.
    async fn send_photon_text(
        &self,
        message: &InboundMessage,
        text: &str,
        force_thread_reply: bool,
    ) -> crate::Result<()> {
        if text.trim().is_empty() {
            return Ok(());
        }
        let reply_to_opt = self.extract_message_id(message);
        let use_reply = force_thread_reply
            || (self.inbound_expects_inline_reply_thread(message) && reply_to_opt.is_some());
        if let (true, Some(reply_to)) = (use_reply, reply_to_opt) {
            self.send_sidecar_command(
                SidecarCommandKind::SendReply,
                json!({
                    "space_id": self.extract_space_id(message)?,
                    "reply_to_message_id": reply_to,
                    "text": text,
                }),
            )
            .await
        } else {
            let space_id = self.extract_space_id(message)?;
            self.send_sidecar_command(
                SidecarCommandKind::SendText,
                json!({
                    "space_id": space_id,
                    "text": text,
                }),
            )
            .await
        }
    }

    async fn send_sidecar_command(
        &self,
        command: SidecarCommandKind,
        payload: serde_json::Value,
    ) -> crate::Result<()> {
        let command_tx = self.command_tx.read().await.clone().ok_or_else(|| {
            crate::Error::Other(anyhow::anyhow!(
                "photon sidecar is not running for adapter '{}'",
                self.runtime_key
            ))
        })?;

        let (response_tx, response_rx) = oneshot::channel::<anyhow::Result<()>>();
        let request = SidecarCommandRequest {
            id: uuid::Uuid::new_v4().to_string(),
            command,
            payload,
            response_tx,
        };

        command_tx.send(request).await.map_err(|error| {
            crate::Error::Other(anyhow::anyhow!(
                "failed to send command to photon sidecar for '{}': {error}",
                self.runtime_key
            ))
        })?;

        match tokio::time::timeout(COMMAND_TIMEOUT, response_rx).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => Err(crate::Error::Other(error)),
            Ok(Err(error)) => Err(crate::Error::Other(anyhow::anyhow!(
                "photon sidecar response channel dropped for '{}': {error}",
                self.runtime_key
            ))),
            Err(_) => Err(crate::Error::Other(anyhow::anyhow!(
                "photon sidecar command timed out for '{}'",
                self.runtime_key
            ))),
        }
    }

    async fn convert_inbound(
        runtime_key: &str,
        dm_allow_list: &HashSet<String>,
        payload: SidecarInboundPayload,
    ) -> Option<InboundMessage> {
        if payload.space_id.trim().is_empty() || payload.sender_id.trim().is_empty() {
            return None;
        }

        if !dm_allow_list.is_empty() && !dm_allow_list.contains(payload.sender_id.as_str()) {
            tracing::debug!(
                adapter = %runtime_key,
                sender_id = %payload.sender_id,
                "dropping photon message from sender outside dm allow-list"
            );
            return None;
        }

        let timestamp = payload
            .timestamp
            .as_deref()
            .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok())
            .map(|timestamp| timestamp.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        let conversation_id = apply_runtime_adapter_to_conversation_id(
            runtime_key,
            format!("{PHOTON_SOURCE}:{}", payload.space_id),
        );

        let mut metadata = HashMap::new();
        metadata.insert(
            "photon_space_id".to_string(),
            serde_json::Value::String(payload.space_id.clone()),
        );
        metadata.insert(
            "photon_message_id".to_string(),
            serde_json::Value::String(payload.message_id.clone()),
        );
        metadata.insert(
            "photon_sender_id".to_string(),
            serde_json::Value::String(payload.sender_id.clone()),
        );
        metadata.insert(
            "photon_mentions_or_replies_to_bot".to_string(),
            serde_json::Value::Bool(payload.mentions_or_replies_to_bot),
        );
        metadata.insert(
            "photon_is_dm".to_string(),
            serde_json::Value::Bool(payload.is_dm),
        );
        metadata.insert(
            metadata_keys::MESSAGE_ID.to_string(),
            serde_json::Value::String(payload.message_id.clone()),
        );
        if let Some(space_name) = payload
            .space_name
            .as_ref()
            .filter(|value| !value.is_empty())
        {
            metadata.insert(
                metadata_keys::CHANNEL_NAME.to_string(),
                serde_json::Value::String(space_name.clone()),
            );
        }
        if let Some(server_name) = payload
            .server_name
            .as_ref()
            .filter(|value| !value.is_empty())
        {
            metadata.insert(
                metadata_keys::SERVER_NAME.to_string(),
                serde_json::Value::String(server_name.clone()),
            );
        }
        if let Some(sender_display_name) = payload
            .sender_display_name
            .as_ref()
            .filter(|value| !value.is_empty())
        {
            metadata.insert(
                "sender_display_name".to_string(),
                serde_json::Value::String(sender_display_name.clone()),
            );
        }

        let attachments = payload
            .attachments
            .into_iter()
            .filter_map(|attachment| {
                let url = if let Some(b64) = attachment
                    .data_base64
                    .as_ref()
                    .map(|raw| raw.trim())
                    .filter(|raw| !raw.is_empty())
                {
                    format!("data:{};base64,{}", attachment.mime_type, b64)
                } else if let Some(ref http_url) = attachment.url {
                    let trimmed = http_url.trim();
                    if trimmed.is_empty() {
                        return None;
                    }
                    trimmed.to_string()
                } else {
                    return None;
                };
                Some(Attachment {
                    filename: attachment.filename,
                    mime_type: attachment.mime_type,
                    url,
                    size_bytes: attachment.size_bytes,
                    auth_header: None,
                    pre_saved_id: None,
                })
            })
            .collect::<Vec<_>>();

        let content = if attachments.is_empty() {
            MessageContent::Text(payload.text)
        } else {
            MessageContent::Media {
                text: if payload.text.is_empty() {
                    None
                } else {
                    Some(payload.text)
                },
                attachments,
            }
        };

        Some(InboundMessage {
            id: payload.message_id.clone(),
            source: PHOTON_SOURCE.to_string(),
            adapter: Some(runtime_key.to_string()),
            conversation_id,
            sender_id: payload.sender_id,
            agent_id: None,
            content,
            timestamp,
            metadata,
            formatted_author: payload.sender_display_name,
        })
    }
}

impl Messaging for PhotonAdapter {
    fn name(&self) -> &str {
        &self.runtime_key
    }

    async fn start(&self) -> crate::Result<InboundStream> {
        let (inbound_tx, inbound_rx) = mpsc::channel(256);
        let (command_tx, mut command_rx) = mpsc::channel::<SidecarCommandRequest>(128);
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        *self.command_tx.write().await = Some(command_tx);
        *self.shutdown_tx.write().await = Some(shutdown_tx);

        let runtime_key = self.runtime_key.clone();
        let project_id = self.project_id.clone();
        let project_secret = self.project_secret.clone();
        let sidecar_command = self.sidecar_command.clone();
        let sidecar_working_dir = self.sidecar_working_dir.clone();
        let dm_allow_list = self.dm_allowed_users.clone();

        tokio::spawn(async move {
            let mut reconnect_backoff = INITIAL_RECONNECT_BACKOFF;

            'supervisor: loop {
                if shutdown_rx.try_recv().is_ok() {
                    break 'supervisor;
                }

                let mut command_builder = if let Some(custom_command) = &sidecar_command {
                    let mut command = Command::new("sh");
                    command.arg("-lc");
                    command.arg(custom_command);
                    command
                } else {
                    let mut command = Command::new("bun");
                    command.arg("run");
                    command.arg("start");
                    command
                };

                if let Some(working_dir) = &sidecar_working_dir {
                    command_builder.current_dir(working_dir);
                } else {
                    command_builder.current_dir("packages/photon-bridge");
                }

                command_builder
                    .env("PHOTON_PROJECT_ID", &project_id)
                    .env("PHOTON_PROJECT_SECRET", &project_secret)
                    .env("PHOTON_ADAPTER_KEY", &runtime_key);
                if !dm_allow_list.is_empty() {
                    command_builder.env(
                        "PHOTON_DM_ALLOW_LIST",
                        dm_allow_list.iter().cloned().collect::<Vec<_>>().join(","),
                    );
                }

                command_builder.stdin(std::process::Stdio::piped());
                command_builder.stdout(std::process::Stdio::piped());
                command_builder.stderr(std::process::Stdio::piped());

                let mut child = match command_builder.spawn() {
                    Ok(child) => child,
                    Err(error) => {
                        tracing::error!(
                            adapter = %runtime_key,
                            %error,
                            "failed to spawn photon sidecar process"
                        );
                        tokio::time::sleep(reconnect_backoff).await;
                        reconnect_backoff = (reconnect_backoff * 2).min(MAX_RECONNECT_BACKOFF);
                        continue;
                    }
                };

                reconnect_backoff = INITIAL_RECONNECT_BACKOFF;

                let Some(mut child_stdin) = child.stdin.take() else {
                    tracing::error!(
                        adapter = %runtime_key,
                        "photon sidecar stdin unavailable"
                    );
                    if let Err(error) = child.kill().await {
                        tracing::warn!(adapter = %runtime_key, %error, "failed to kill photon child");
                    }
                    tokio::time::sleep(INITIAL_RECONNECT_BACKOFF).await;
                    continue;
                };
                let Some(child_stdout) = child.stdout.take() else {
                    tracing::error!(
                        adapter = %runtime_key,
                        "photon sidecar stdout unavailable"
                    );
                    if let Err(error) = child.kill().await {
                        tracing::warn!(adapter = %runtime_key, %error, "failed to kill photon child");
                    }
                    tokio::time::sleep(INITIAL_RECONNECT_BACKOFF).await;
                    continue;
                };
                let Some(child_stderr) = child.stderr.take() else {
                    tracing::error!(
                        adapter = %runtime_key,
                        "photon sidecar stderr unavailable"
                    );
                    if let Err(error) = child.kill().await {
                        tracing::warn!(adapter = %runtime_key, %error, "failed to kill photon child");
                    }
                    tokio::time::sleep(INITIAL_RECONNECT_BACKOFF).await;
                    continue;
                };

                let stderr_runtime_key = runtime_key.clone();
                tokio::spawn(async move {
                    let mut stderr_lines = BufReader::new(child_stderr).lines();
                    loop {
                        match stderr_lines.next_line().await {
                            Ok(Some(line)) => {
                                if !line.trim().is_empty() {
                                    tracing::debug!(
                                        adapter = %stderr_runtime_key,
                                        sidecar_stderr = %line,
                                        "photon sidecar stderr"
                                    );
                                }
                            }
                            Ok(None) => break,
                            Err(error) => {
                                tracing::debug!(
                                    adapter = %stderr_runtime_key,
                                    %error,
                                    "failed reading photon sidecar stderr"
                                );
                                break;
                            }
                        }
                    }
                });

                tracing::info!(adapter = %runtime_key, "photon sidecar started");

                let mut stdout_lines = BufReader::new(child_stdout).lines();
                let mut pending_commands: HashMap<String, oneshot::Sender<anyhow::Result<()>>> =
                    HashMap::new();

                loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            let shutdown_envelope = SidecarCommandEnvelope {
                                id: uuid::Uuid::new_v4().to_string(),
                                command: SidecarCommandKind::Shutdown,
                                payload: serde_json::Value::Null,
                            };
                            match serialize_command_line(&shutdown_envelope) {
                                Ok(line) => {
                                    if let Err(error) = child_stdin.write_all(line.as_bytes()).await {
                                        tracing::debug!(adapter = %runtime_key, %error, "failed to send sidecar shutdown command");
                                    }
                                    if let Err(error) = child_stdin.write_all(b"\n").await {
                                        tracing::debug!(adapter = %runtime_key, %error, "failed to terminate sidecar shutdown command line");
                                    }
                                    if let Err(error) = child_stdin.flush().await {
                                        tracing::debug!(adapter = %runtime_key, %error, "failed to flush sidecar shutdown command");
                                    }
                                }
                                Err(error) => {
                                    tracing::debug!(adapter = %runtime_key, %error, "failed to serialize sidecar shutdown command");
                                }
                            }
                            if let Err(error) = child.kill().await {
                                tracing::debug!(adapter = %runtime_key, %error, "failed to kill photon sidecar during shutdown");
                            }
                            for (_id, response_tx) in pending_commands.drain() {
                                if response_tx
                                    .send(Err(anyhow::anyhow!("photon sidecar shutting down")))
                                    .is_err()
                                {
                                    tracing::debug!(
                                        adapter = %runtime_key,
                                        "failed to return sidecar shutdown error to caller"
                                    );
                                }
                            }
                            break 'supervisor;
                        }
                        request = command_rx.recv() => {
                            let Some(request) = request else {
                                if let Err(error) = child.kill().await {
                                    tracing::debug!(adapter = %runtime_key, %error, "failed to kill photon sidecar after command channel close");
                                }
                                for (_id, response_tx) in pending_commands.drain() {
                                    if response_tx
                                        .send(Err(anyhow::anyhow!("photon command channel closed")))
                                        .is_err()
                                    {
                                        tracing::debug!(
                                            adapter = %runtime_key,
                                            "failed to return sidecar command-close error to caller"
                                        );
                                    }
                                }
                                break 'supervisor;
                            };

                            let envelope = SidecarCommandEnvelope {
                                id: request.id.clone(),
                                command: request.command,
                                payload: request.payload,
                            };
                            let serialized = match serialize_command_line(&envelope) {
                                Ok(serialized) => serialized,
                                Err(error) => {
                                    if request.response_tx.send(Err(error)).is_err() {
                                        tracing::debug!(adapter = %runtime_key, "failed to return sidecar serialization error to caller");
                                    }
                                    continue;
                                }
                            };

                            if let Err(error) = child_stdin.write_all(serialized.as_bytes()).await {
                                if request.response_tx.send(Err(anyhow::anyhow!(
                                    "failed writing command to photon sidecar: {error}"
                                ))).is_err() {
                                    tracing::debug!(adapter = %runtime_key, "failed to return sidecar write error to caller");
                                }
                                continue;
                            }
                            if let Err(error) = child_stdin.write_all(b"\n").await {
                                if request.response_tx.send(Err(anyhow::anyhow!(
                                    "failed writing command newline to photon sidecar: {error}"
                                ))).is_err() {
                                    tracing::debug!(adapter = %runtime_key, "failed to return sidecar newline error to caller");
                                }
                                continue;
                            }
                            if let Err(error) = child_stdin.flush().await {
                                if request.response_tx.send(Err(anyhow::anyhow!(
                                    "failed flushing command to photon sidecar: {error}"
                                ))).is_err() {
                                    tracing::debug!(adapter = %runtime_key, "failed to return sidecar flush error to caller");
                                }
                                continue;
                            }

                            pending_commands.insert(request.id, request.response_tx);
                        }
                        stdout_line = stdout_lines.next_line() => {
                            match stdout_line {
                                Ok(Some(line)) => {
                                    if line.trim().is_empty() {
                                        continue;
                                    }
                                    let event = match serde_json::from_str::<SidecarEvent>(&line) {
                                        Ok(event) => event,
                                        Err(error) => {
                                            tracing::warn!(
                                                adapter = %runtime_key,
                                                %error,
                                                line = %line,
                                                "failed to parse photon sidecar event"
                                            );
                                            continue;
                                        }
                                    };

                                    match event {
                                        SidecarEvent::Ready { adapter_key } => {
                                            tracing::info!(
                                                adapter = %runtime_key,
                                                ready_adapter_key = ?adapter_key,
                                                "photon sidecar ready"
                                            );
                                        }
                                        SidecarEvent::Inbound { payload } => {
                                            if let Some(message) = PhotonAdapter::convert_inbound(
                                                runtime_key.as_str(),
                                                &dm_allow_list,
                                                payload,
                                            )
                                            .await
                                                && inbound_tx.send(message).await.is_err()
                                            {
                                                tracing::warn!(adapter = %runtime_key, "photon inbound channel closed");
                                                break;
                                            }
                                        }
                                        SidecarEvent::Response { id, ok, error } => {
                                            if let Some(response_tx) = pending_commands.remove(&id) {
                                                let send_result = if ok {
                                                    response_tx.send(Ok(()))
                                                } else {
                                                    response_tx.send(Err(anyhow::anyhow!(
                                                        "{}",
                                                        error.unwrap_or_else(|| "unknown photon sidecar error".to_string())
                                                    )))
                                                };
                                                if send_result.is_err() {
                                                    tracing::debug!(adapter = %runtime_key, response_id = %id, "sidecar response receiver dropped");
                                                }
                                            }
                                        }
                                        SidecarEvent::Log { level, message } => {
                                            match level.as_deref() {
                                                Some("error") => tracing::error!(adapter = %runtime_key, sidecar_log = %message),
                                                Some("warn") => tracing::warn!(adapter = %runtime_key, sidecar_log = %message),
                                                _ => tracing::debug!(adapter = %runtime_key, sidecar_log = %message),
                                            }
                                        }
                                    }
                                }
                                Ok(None) => {
                                    tracing::warn!(adapter = %runtime_key, "photon sidecar stdout closed");
                                    break;
                                }
                                Err(error) => {
                                    tracing::warn!(adapter = %runtime_key, %error, "failed reading photon sidecar stdout");
                                    break;
                                }
                            }
                        }
                    }
                }

                for (_id, response_tx) in pending_commands.drain() {
                    if response_tx
                        .send(Err(anyhow::anyhow!(
                            "photon sidecar disconnected before responding"
                        )))
                        .is_err()
                    {
                        tracing::debug!(
                            adapter = %runtime_key,
                            "failed to return sidecar disconnect error to caller"
                        );
                    }
                }

                if shutdown_rx.try_recv().is_ok() {
                    break 'supervisor;
                }

                tracing::warn!(
                    adapter = %runtime_key,
                    backoff_secs = reconnect_backoff.as_secs(),
                    "photon sidecar disconnected; restarting with backoff"
                );
                tokio::time::sleep(reconnect_backoff).await;
                reconnect_backoff = (reconnect_backoff * 2).min(MAX_RECONNECT_BACKOFF);
            }

            tracing::info!(adapter = %runtime_key, "photon sidecar supervisor stopped");
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(inbound_rx);
        Ok(Box::pin(stream))
    }

    async fn respond(
        &self,
        message: &InboundMessage,
        response: OutboundResponse,
    ) -> crate::Result<()> {
        match response {
            OutboundResponse::Text(text) => {
                self.send_photon_text(message, text.as_str(), false).await?;
            }
            OutboundResponse::RichMessage { text, .. } => {
                self.send_photon_text(message, text.as_str(), false).await?;
            }
            OutboundResponse::ThreadReply { text, .. } => {
                self.send_photon_text(message, text.as_str(), true).await?;
            }
            OutboundResponse::File {
                filename,
                data,
                mime_type,
                caption,
            } => {
                let space_id = self.extract_space_id(message)?;
                let reply_to_opt = match (
                    self.inbound_expects_inline_reply_thread(message),
                    self.extract_message_id(message),
                ) {
                    (true, Some(identifier)) => Some(identifier),
                    _ => None,
                };
                let base64_blob = base64::engine::general_purpose::STANDARD.encode(data);
                if let Some(identifier) = reply_to_opt.as_ref() {
                    self.send_sidecar_command(
                        SidecarCommandKind::SendFile,
                        json!({
                            "space_id": space_id,
                            "reply_to_message_id": identifier,
                            "filename": filename,
                            "mime_type": mime_type,
                            "data_base64": base64_blob,
                            "caption": caption,
                        }),
                    )
                    .await?;
                } else {
                    self.send_sidecar_command(
                        SidecarCommandKind::SendFile,
                        json!({
                            "space_id": space_id,
                            "filename": filename,
                            "mime_type": mime_type,
                            "data_base64": base64_blob,
                            "caption": caption,
                        }),
                    )
                    .await?;
                }
            }
            OutboundResponse::Reaction(emoji) => {
                if let Some(message_id) = self.extract_message_id(message) {
                    let space_id = self.extract_space_id(message)?;
                    self.send_sidecar_command(
                        SidecarCommandKind::AddReaction,
                        json!({
                            "space_id": space_id,
                            "message_id": message_id,
                            "emoji": emoji,
                        }),
                    )
                    .await?;
                }
            }
            OutboundResponse::RemoveReaction(emoji) => {
                if let Some(message_id) = self.extract_message_id(message) {
                    let space_id = self.extract_space_id(message)?;
                    self.send_sidecar_command(
                        SidecarCommandKind::RemoveReaction,
                        json!({
                            "space_id": space_id,
                            "message_id": message_id,
                            "emoji": emoji,
                        }),
                    )
                    .await?;
                }
            }
            OutboundResponse::Ephemeral { text, .. } => {
                self.send_photon_text(message, text.as_str(), false).await?;
            }
            OutboundResponse::ScheduledMessage { text, .. } => {
                self.send_photon_text(message, text.as_str(), false).await?;
            }
            OutboundResponse::StreamStart => {
                self.stream_buffers
                    .write()
                    .await
                    .insert(message.conversation_id.clone(), String::new());
            }
            OutboundResponse::StreamChunk(chunk) => {
                let mut stream_buffers = self.stream_buffers.write().await;
                let entry = stream_buffers
                    .entry(message.conversation_id.clone())
                    .or_insert_with(String::new);
                entry.push_str(chunk.as_str());
            }
            OutboundResponse::StreamEnd => {
                if let Some(content) = self
                    .stream_buffers
                    .write()
                    .await
                    .remove(message.conversation_id.as_str())
                {
                    self.send_photon_text(message, content.as_str(), false)
                        .await?;
                }
            }
            OutboundResponse::Status(status) => {
                self.send_status(message, status).await?;
            }
        }

        Ok(())
    }

    async fn send_status(
        &self,
        message: &InboundMessage,
        status: StatusUpdate,
    ) -> crate::Result<()> {
        let space_id = self.extract_space_id(message)?;
        match status {
            StatusUpdate::Thinking => {
                self.send_sidecar_command(
                    SidecarCommandKind::StartTyping,
                    json!({
                        "space_id": space_id,
                    }),
                )
                .await?;
            }
            _ => {
                self.send_sidecar_command(
                    SidecarCommandKind::StopTyping,
                    json!({
                        "space_id": space_id,
                    }),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn broadcast(&self, target: &str, response: OutboundResponse) -> crate::Result<()> {
        let normalized_target = target.trim();
        if normalized_target.is_empty() {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "photon broadcast target can't be empty"
            )));
        }

        match response {
            OutboundResponse::Text(text)
            | OutboundResponse::ThreadReply { text, .. }
            | OutboundResponse::Ephemeral { text, .. }
            | OutboundResponse::ScheduledMessage { text, .. }
            | OutboundResponse::RichMessage { text, .. } => {
                self.send_sidecar_command(
                    SidecarCommandKind::SendText,
                    json!({
                        "space_id": normalized_target,
                        "text": text,
                    }),
                )
                .await
            }
            OutboundResponse::File {
                filename,
                data,
                mime_type,
                caption,
            } => {
                self.send_sidecar_command(
                    SidecarCommandKind::SendFile,
                    json!({
                        "space_id": normalized_target,
                        "filename": filename,
                        "mime_type": mime_type,
                        "data_base64": base64::engine::general_purpose::STANDARD.encode(data),
                        "caption": caption,
                    }),
                )
                .await
            }
            OutboundResponse::Reaction(_)
            | OutboundResponse::RemoveReaction(_)
            | OutboundResponse::StreamStart
            | OutboundResponse::StreamChunk(_)
            | OutboundResponse::StreamEnd
            | OutboundResponse::Status(_) => Ok(()),
        }
    }

    async fn fetch_history(
        &self,
        _message: &InboundMessage,
        _limit: usize,
    ) -> crate::Result<Vec<HistoryMessage>> {
        Ok(Vec::new())
    }

    async fn health_check(&self) -> crate::Result<()> {
        self.send_sidecar_command(SidecarCommandKind::Health, serde_json::Value::Null)
            .await
    }

    async fn shutdown(&self) -> crate::Result<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.write().await.take() {
            shutdown_tx.try_send(()).ok();
        }
        *self.command_tx.write().await = None;
        self.stream_buffers.write().await.clear();
        tracing::info!(adapter = %self.runtime_key, "photon adapter shut down");
        Ok(())
    }
}

#[derive(Debug)]
struct SidecarCommandRequest {
    id: String,
    command: SidecarCommandKind,
    payload: serde_json::Value,
    response_tx: oneshot::Sender<anyhow::Result<()>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum SidecarCommandKind {
    SendText,
    SendReply,
    SendFile,
    AddReaction,
    RemoveReaction,
    StartTyping,
    StopTyping,
    Health,
    Shutdown,
}

#[derive(Debug, Clone, Serialize)]
struct SidecarCommandEnvelope {
    id: String,
    command: SidecarCommandKind,
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SidecarEvent {
    Ready {
        adapter_key: Option<String>,
    },
    Inbound {
        payload: SidecarInboundPayload,
    },
    Response {
        id: String,
        ok: bool,
        error: Option<String>,
    },
    Log {
        level: Option<String>,
        message: String,
    },
}

#[derive(Debug, Deserialize)]
struct SidecarInboundPayload {
    space_id: String,
    message_id: String,
    sender_id: String,
    #[serde(default)]
    sender_display_name: Option<String>,
    #[serde(default)]
    text: String,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    mentions_or_replies_to_bot: bool,
    #[serde(default)]
    is_dm: bool,
    #[serde(default)]
    space_name: Option<String>,
    #[serde(default)]
    server_name: Option<String>,
    #[serde(default)]
    attachments: Vec<SidecarInboundAttachment>,
}

#[derive(Debug, Deserialize)]
struct SidecarInboundAttachment {
    filename: String,
    mime_type: String,
    /// HTTP(S) fetch URL when present; omit or `null` when only `data_base64` is set.
    #[serde(default)]
    url: Option<String>,
    /// Inline bytes from the Spectrum sidecar (`read()`), as standard base64.
    #[serde(default)]
    data_base64: Option<String>,
    #[serde(default)]
    size_bytes: Option<u64>,
}

fn serialize_command_line(command: &SidecarCommandEnvelope) -> anyhow::Result<String> {
    serde_json::to_string(command).context("failed to serialize photon sidecar command")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn convert_inbound_sets_runtime_adapter_and_metadata() {
        let payload = SidecarInboundPayload {
            space_id: "space-123".to_string(),
            message_id: "msg-456".to_string(),
            sender_id: "sender-1".to_string(),
            sender_display_name: Some("Alice".to_string()),
            text: "hello".to_string(),
            timestamp: Some("2026-01-01T00:00:00Z".to_string()),
            mentions_or_replies_to_bot: true,
            is_dm: true,
            space_name: Some("Support".to_string()),
            server_name: Some("iMessage".to_string()),
            attachments: Vec::new(),
        };

        let inbound = PhotonAdapter::convert_inbound("photon:support", &HashSet::new(), payload)
            .await
            .expect("inbound should parse");

        assert_eq!(inbound.source, "photon");
        assert_eq!(inbound.adapter.as_deref(), Some("photon:support"));
        assert_eq!(inbound.conversation_id, "photon:support:space-123");
        assert_eq!(
            inbound
                .metadata
                .get("photon_mentions_or_replies_to_bot")
                .and_then(|value| value.as_bool()),
            Some(true)
        );
        assert_eq!(
            inbound
                .metadata
                .get("photon_is_dm")
                .and_then(|value| value.as_bool()),
            Some(true)
        );
    }

    #[tokio::test]
    async fn convert_inbound_inline_base64_becomes_data_url_attachment() {
        let payload = SidecarInboundPayload {
            space_id: "space-123".to_string(),
            message_id: "msg-456".to_string(),
            sender_id: "sender-1".to_string(),
            sender_display_name: None,
            text: "check this".to_string(),
            timestamp: None,
            mentions_or_replies_to_bot: false,
            is_dm: true,
            space_name: None,
            server_name: None,
            attachments: vec![SidecarInboundAttachment {
                filename: "pic.png".to_string(),
                mime_type: "image/png".to_string(),
                url: None,
                data_base64: Some("aGVsbG8=".to_string()),
                size_bytes: Some(5),
            }],
        };

        let inbound = PhotonAdapter::convert_inbound("photon", &HashSet::new(), payload)
            .await
            .expect("inbound should parse");

        match inbound.content {
            MessageContent::Media { text, attachments } => {
                assert_eq!(text.as_deref(), Some("check this"));
                assert_eq!(attachments.len(), 1);
                assert!(
                    attachments[0]
                        .url
                        .starts_with("data:image/png;base64,aGVsbG8=")
                );
            }
            other => panic!("expected Media, got {other:?}"),
        }
    }

    #[test]
    fn deserializes_sidecar_payload_when_attachment_has_only_data_base64() {
        let line = r#"{"type":"inbound","payload":{"space_id":"s1","message_id":"m1","sender_id":"u1","text":"","mentions_or_replies_to_bot":false,"is_dm":true,"attachments":[{"filename":"IMG.JPG","mime_type":"image/jpeg","data_base64":"YQo=","size_bytes":461663}]}}"#;
        let event: SidecarEvent = serde_json::from_str(line).expect("sidecar JSON should parse");
        match event {
            SidecarEvent::Inbound { payload } => {
                assert_eq!(payload.attachments.len(), 1);
                let att = &payload.attachments[0];
                assert_eq!(att.filename, "IMG.JPG");
                assert_eq!(att.mime_type, "image/jpeg");
                assert!(att.url.is_none());
                assert_eq!(att.data_base64.as_deref(), Some("YQo="));
                assert_eq!(att.size_bytes, Some(461663));
            }
            other => panic!("expected Inbound, got {other:?}"),
        }
    }

    #[test]
    fn serialize_command_line_emits_command_and_payload() {
        let line = serialize_command_line(&SidecarCommandEnvelope {
            id: "abc".to_string(),
            command: SidecarCommandKind::SendText,
            payload: json!({
                "space_id": "space-1",
                "text": "hello",
            }),
        })
        .expect("command should serialize");

        let value: serde_json::Value =
            serde_json::from_str(line.as_str()).expect("serialized line should be valid json");
        assert_eq!(value.get("id").and_then(|item| item.as_str()), Some("abc"));
        assert_eq!(
            value.get("command").and_then(|item| item.as_str()),
            Some("send_text")
        );
        assert_eq!(
            value
                .get("payload")
                .and_then(|item| item.get("space_id"))
                .and_then(|item| item.as_str()),
            Some("space-1")
        );
    }
}
