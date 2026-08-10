//! Durable Hub application state and product use cases.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use mews_agent::AgentCapabilities;
use mews_relay::{RelayAdmission, RelayPeerId};
use serde_json::Value;

use crate::{
    Agent, MessageContent, MessageRole, MessageSource, RunStatus, Session, SourceKind,
    host::{ConnectedHost, HostExecutor},
    identity::{HostIdentity, NoiseIdentity},
};
use mews_host::ToolRegistry;
use mews_store::Store;

pub const DEFAULT_SOUL: &str =
    "You are a capable, practical agent. Be concise and use tools when they help.";
#[cfg(test)]
pub const DEFAULT_CONFIG: &str = "harness = \"mews\"\ntools = [\"*\"]\n";
const REVISION_FILE: &str = ".revision";
const DATABASE_FILE: &str = "mews.db";

pub struct Mews {
    root: PathBuf,
    store: Store,
}

pub struct HubSnapshot {
    pub move_nonce: String,
    pub installation_id: crate::InstallationId,
    pub generation: u64,
    pub database_path: PathBuf,
    pub database_size: u64,
    pub database_sha256: String,
    pub installation_key: Vec<u8>,
    pub hub_noise_key: Vec<u8>,
    pub credentials: Vec<u8>,
    pub previous_hub: crate::HostId,
    pub target_hub: crate::HostId,
}

mod acp;
mod agents;
mod core;
mod handoff;
mod native;
mod providers;
mod runs;
mod sessions;
pub(crate) use runs::StartedRun;

#[cfg(test)]
use sessions::{parse_prompt_arguments, substitute_prompt_arguments};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_places_keys_in_the_secrets_directory() {
        let root = tempfile::tempdir().unwrap();

        Mews::setup(root.path(), "laptop").unwrap();

        for name in [
            "host.key",
            "host-noise.key",
            "hub-noise.key",
            "installation.key",
        ] {
            assert!(root.path().join("secrets").join(name).is_file());
            assert!(!root.path().join(format!(".{name}")).exists());
        }
        assert!(root.path().join("logs").is_dir());
    }

    #[test]
    fn invitation_uses_configured_relay_by_default() {
        let root = tempfile::tempdir().unwrap();
        let mews = Mews::setup(root.path(), "laptop").unwrap();
        mews.set_relay_url("ws://laptop.local:8787").unwrap();

        let offer = mews.create_invitation(None).unwrap();

        assert_eq!(offer.relay_url, "ws://laptop.local:8787");
    }

    #[test]
    fn new_agents_copy_the_installation_provider_defaults() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        mews.store.set_default_model("openai/gpt-test").unwrap();
        mews.store
            .set_default_reasoning(Some(crate::ReasoningEffort::High))
            .unwrap();
        let agent = mews.create_agent("coder").unwrap();
        let config = crate::AgentConfig::parse(
            &fs::read_to_string(root.path().join("agents/coder/agent.toml")).unwrap(),
        )
        .unwrap();
        assert_eq!(config.harness, "mews");
        assert_eq!(
            config.harness_options.get("model").map(String::as_str),
            Some("openai/gpt-test")
        );
        assert_eq!(
            config.harness_options.get("reasoning").map(String::as_str),
            Some("high")
        );
        let session = mews
            .store
            .create_session(
                &agent.id,
                &mews.installation().unwrap().hub_host_id,
                root.path(),
            )
            .unwrap();
        assert_eq!(
            mews.session_model_config(&session).unwrap(),
            crate::SessionModelConfig {
                harness: "mews".into(),
                model: Some("openai/gpt-test".into()),
                reasoning: Some(crate::ReasoningEffort::High),
            }
        );
        let session = mews
            .set_session_model(&session.id, Some("anthropic/claude-test"))
            .unwrap();
        assert_eq!(
            mews.session_model_config(&session).unwrap().model,
            Some("anthropic/claude-test".into())
        );
    }

    #[tokio::test]
    async fn native_provider_defaults_reject_auto_reasoning() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();

        let error = mews
            .set_default_reasoning(Some(crate::ReasoningEffort::Auto))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("use Provider default instead"));

        mews.store
            .set_default_reasoning(Some(crate::ReasoningEffort::Auto))
            .unwrap();
        let error = mews.create_agent("coder").unwrap_err();
        assert!(error.to_string().contains("use Provider default instead"));
    }

    #[test]
    fn session_history_reads_the_active_timeline() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        let agent = mews.create_agent("coder").unwrap();
        let session = mews
            .store
            .create_session(
                &agent.id,
                &mews.installation().unwrap().hub_host_id,
                root.path(),
            )
            .unwrap();
        let source = MessageSource {
            kind: SourceKind::Client,
            id: "cli".into(),
            channel_origin: None,
        };
        mews.store
            .append_message(
                &session.id,
                MessageRole::User,
                MessageContent::Text {
                    text: "remember maple".into(),
                },
                Value::Null,
                source,
            )
            .unwrap();

        let history = mews.session_history(&session.id).unwrap();
        assert!(matches!(
            history.as_slice(),
            [message] if matches!(&message.content, MessageContent::Text { text } if text == "remember maple")
        ));
    }

    #[test]
    fn rename_agent_moves_the_editable_replica_after_synchronizing_it() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        mews.create_agent("coder").unwrap();
        std::fs::create_dir_all(root.path().join("agents/coder/skills/review")).unwrap();
        std::fs::write(
            root.path().join("agents/coder/skills/review/SKILL.md"),
            "skill",
        )
        .unwrap();
        std::fs::write(root.path().join("agents/coder/SOUL.md"), "locally edited").unwrap();

        let renamed = mews.rename_agent("coder", "builder").unwrap();

        assert_eq!(renamed.slug, "builder");
        assert!(!root.path().join("agents/coder").exists());
        assert_eq!(
            std::fs::read_to_string(root.path().join("agents/builder/SOUL.md")).unwrap(),
            "locally edited"
        );
        assert_eq!(
            mews.agent_revision(&renamed).unwrap().soul,
            "locally edited"
        );
        assert_eq!(
            std::fs::read_to_string(root.path().join("agents/builder/skills/review/SKILL.md"))
                .unwrap(),
            "skill"
        );
    }

    #[test]
    fn synchronizing_an_agent_preserves_its_local_skills() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        mews.create_agent("coder").unwrap();
        std::fs::create_dir_all(root.path().join("agents/coder/skills/review")).unwrap();
        std::fs::write(
            root.path().join("agents/coder/skills/review/SKILL.md"),
            "skill",
        )
        .unwrap();
        std::fs::remove_file(root.path().join("agents/coder/SOUL.md")).unwrap();

        mews.synchronize_agent("coder").unwrap();

        assert_eq!(
            std::fs::read_to_string(root.path().join("agents/coder/skills/review/SKILL.md"))
                .unwrap(),
            "skill"
        );
    }

    #[test]
    fn new_agents_preserve_the_requested_harness_and_its_options() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        mews.store.set_default_model("openai/gpt-test").unwrap();
        let agent = mews
            .create_agent_with_harness(
                "coder",
                "codex",
                std::collections::BTreeMap::from([
                    ("model".into(), "gpt-5.6-codex".into()),
                    ("approval".into(), "never".into()),
                    ("reasoning_effort".into(), "medium".into()),
                ]),
            )
            .unwrap();

        let config = crate::AgentConfig::parse(
            &fs::read_to_string(root.path().join("agents/coder/agent.toml")).unwrap(),
        )
        .unwrap();
        assert_eq!(agent.slug, "coder");
        assert_eq!(config.harness, "codex");
        assert_eq!(
            config.harness_options,
            std::collections::BTreeMap::from([
                ("model".into(), "gpt-5.6-codex".into()),
                ("approval".into(), "never".into()),
                ("reasoning_effort".into(), "medium".into()),
            ])
        );

        let session = mews
            .store
            .create_session(
                &agent.id,
                &mews.installation().unwrap().hub_host_id,
                root.path(),
            )
            .unwrap();
        assert_eq!(
            mews.session_model_config(&session).unwrap(),
            crate::SessionModelConfig {
                harness: "codex".into(),
                model: Some("gpt-5.6-codex".into()),
                reasoning: Some(crate::ReasoningEffort::Medium),
            }
        );
        let error = mews
            .set_session_model(&session.id, Some("gpt-5.6-codex-mini"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("only supported by the native mews Harness"));
        assert_eq!(
            mews.store.session(&session.id).unwrap().model_override,
            None
        );
    }

    #[tokio::test]
    async fn agents_can_be_authored_without_a_provider_but_runs_explain_setup() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        mews.create_agent("coder").unwrap();
        let config = crate::AgentConfig::parse(
            &fs::read_to_string(root.path().join("agents/coder/agent.toml")).unwrap(),
        )
        .unwrap();
        assert_eq!(config.harness, "mews");
        assert!(config.harness_options.is_empty());

        let session = mews.start_session("coder", root.path()).await.unwrap();
        let error = mews
            .send(&session, "hello", Value::Null)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("mews providers login"));
        assert!(mews.store.messages(&session.id).unwrap().is_empty());

        mews.store.set_default_model("openai/gpt-test").unwrap();
        assert_eq!(
            mews.session_model_config(&session)
                .unwrap()
                .model
                .as_deref(),
            Some("openai/gpt-test")
        );
    }

    #[tokio::test]
    async fn unresolved_harnesses_fail_without_a_native_fallback() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        let agent = mews.create_agent("coder").unwrap();
        let current = mews
            .store
            .agent_revision(&agent.id, agent.current_revision)
            .unwrap();
        mews.store
            .update_agent(
                &agent.id,
                current.revision,
                &current.soul,
                "harness = \"codex\"\n",
                &mews.installation().unwrap().hub_host_id,
            )
            .unwrap();
        let session = mews
            .store
            .create_session(
                &agent.id,
                &mews.installation().unwrap().hub_host_id,
                root.path(),
            )
            .unwrap();

        let error = mews
            .send(&session, "hello", Value::Null)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("Harness \"codex\" is not ready"));
    }

    #[test]
    fn signed_join_request_consumes_offer_once() {
        let hub_root = tempfile::tempdir().unwrap();
        let joining_root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(hub_root.path(), "laptop").unwrap();
        let offer = mews.create_invitation(Some("ws://127.0.0.1:9000")).unwrap();
        let host =
            HostIdentity::load_or_create(&joining_root.path().join("secrets/host.key")).unwrap();
        let noise =
            NoiseIdentity::load_or_create(&joining_root.path().join("secrets/host-noise.key"))
                .unwrap();
        let request = crate::enrollment::JoinRequest::create(
            &offer,
            "mini-pc".into(),
            &host,
            &noise,
            "ws://mini-pc.local:8787".into(),
        );
        let enrolled = mews.enroll_host(&offer, &request).unwrap();
        assert_eq!(enrolled.public_key, host.public_key());
        assert_eq!(enrolled.noise_public_key, noise.public_key());
        assert!(mews.enroll_host(&offer, &request).is_err());
    }

    #[tokio::test]
    async fn wrong_host_cannot_mutate_session_history() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        mews.create_agent("coder").unwrap();
        let session = mews.start_session("coder", root.path()).await.unwrap();

        let joining_root = tempfile::tempdir().unwrap();
        let offer = mews.create_invitation(Some("ws://127.0.0.1:9000")).unwrap();
        let identity =
            HostIdentity::load_or_create(&joining_root.path().join("secrets/host.key")).unwrap();
        let noise =
            NoiseIdentity::load_or_create(&joining_root.path().join("secrets/host-noise.key"))
                .unwrap();
        let request = crate::enrollment::JoinRequest::create(
            &offer,
            "mini-pc".into(),
            &identity,
            &noise,
            "ws://mini-pc.local:8787".into(),
        );
        let remote = mews.enroll_host(&offer, &request).unwrap();
        let executor = ConnectedHost::in_process(remote.id, ToolRegistry::with_defaults())
            .await
            .unwrap();

        assert!(
            mews.send_on(&session, "must not persist", Value::Null, &executor)
                .await
                .is_err()
        );
        let database = rusqlite::Connection::open(root.path().join("mews.db")).unwrap();
        let messages: u64 = database
            .query_row("SELECT COUNT(*) FROM session_entries", [], |row| row.get(0))
            .unwrap();
        assert_eq!(messages, 0);
    }

    #[test]
    fn hub_snapshot_promotes_target_and_failed_transfer_rolls_source_forward() {
        let root = tempfile::tempdir().unwrap();
        let joining = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        let original = mews.installation().unwrap().hub_host_id;
        let offer = mews.create_invitation(Some("ws://127.0.0.1:9000")).unwrap();
        let identity =
            HostIdentity::load_or_create(&joining.path().join("secrets/host.key")).unwrap();
        let noise =
            NoiseIdentity::load_or_create(&joining.path().join("secrets/host-noise.key")).unwrap();
        let request = crate::enrollment::JoinRequest::create(
            &offer,
            "mini-pc".into(),
            &identity,
            &noise,
            "ws://mini-pc.local:8787".into(),
        );
        let target = mews.enroll_host(&offer, &request).unwrap();

        let snapshot = mews.begin_hub_move(&target.id).unwrap();
        let snapshot_store = Store::open(&snapshot.database_path).unwrap();
        let promoted = snapshot_store.installation().unwrap().unwrap();
        assert_eq!(promoted.hub_host_id, target.id);
        assert_eq!(promoted.generation, 2);

        mews.rollback_hub_move(&snapshot).unwrap();
        let rolled_back = mews.installation().unwrap();
        assert_eq!(rolled_back.hub_host_id, original);
        assert_eq!(rolled_back.generation, 3);
    }

    #[test]
    fn demoted_host_state_contains_trust_material_but_no_join_offer() {
        let root = tempfile::tempdir().unwrap();
        let joining = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        let offer = mews.create_invitation(Some("ws://127.0.0.1:9000")).unwrap();
        let identity =
            HostIdentity::load_or_create(&joining.path().join("secrets/host.key")).unwrap();
        let noise =
            NoiseIdentity::load_or_create(&joining.path().join("secrets/host-noise.key")).unwrap();
        let request = crate::enrollment::JoinRequest::create(
            &offer,
            "mini-pc".into(),
            &identity,
            &noise,
            "ws://mini-pc.local:8787".into(),
        );
        let target = mews.enroll_host(&offer, &request).unwrap();

        let state = mews.demoted_host_state(&target.id).unwrap();
        let mut persisted = serde_json::to_value(&state).unwrap();
        assert_eq!(
            persisted["installation_public_key"],
            state.installation_public_key
        );
        assert_eq!(
            persisted["hub_noise_public_key"],
            state.hub_noise_public_key
        );
        assert!(persisted.get("invitation_id").is_none());
        assert!(persisted.get("secret").is_none());
        assert!(persisted.get("offer").is_none());

        persisted
            .as_object_mut()
            .unwrap()
            .insert("offer".into(), serde_json::to_value(offer).unwrap());
        assert!(
            serde_json::from_value::<crate::enrollment::join::JoinedHostState>(persisted).is_err()
        );
    }

    #[test]
    fn preparing_move_journal_recovers_source_after_process_loss() {
        let root = tempfile::tempdir().unwrap();
        let joining = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        let original = mews.installation().unwrap().hub_host_id;
        let offer = mews.create_invitation(Some("ws://127.0.0.1:9000")).unwrap();
        let identity =
            HostIdentity::load_or_create(&joining.path().join("secrets/host.key")).unwrap();
        let noise =
            NoiseIdentity::load_or_create(&joining.path().join("secrets/host-noise.key")).unwrap();
        let request = crate::enrollment::JoinRequest::create(
            &offer,
            "mini-pc".into(),
            &identity,
            &noise,
            "ws://mini-pc.local:8787".into(),
        );
        let target = mews.enroll_host(&offer, &request).unwrap();
        drop(mews.begin_hub_move(&target.id).unwrap());
        std::fs::write(root.path().join("hub-move.phase"), "preparing").unwrap();
        drop(mews);

        let recovered = Mews::open(root.path()).unwrap().installation().unwrap();
        assert_eq!(recovered.hub_host_id, original);
        assert_eq!(recovered.generation, 3);
    }

    #[test]
    fn prompt_arguments_expand_supported_placeholders() {
        let arguments = parse_prompt_arguments("Button \"click handler\" optional");
        assert_eq!(arguments, ["Button", "click handler", "optional"]);
        assert_eq!(
            substitute_prompt_arguments(
                "$1 | $2 | $@ | ${3:-fallback} | ${4:-fallback} | ${@:2:2}",
                &arguments,
            ),
            "Button | click handler | Button click handler optional | optional | fallback | click handler optional"
        );
    }
}
