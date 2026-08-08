//! Stable serialized contracts shared by MEWS clients, Hub, and Hosts.

mod domain;
mod host;
mod hub;

pub use domain::*;
pub use host::*;
pub use hub::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_frames_round_trip_through_the_shared_contract() {
        let request = HubToHost::Ping { nonce: 42 };
        let decoded: HubToHost = decode(&encode(request).unwrap()).unwrap();
        assert!(matches!(decoded, HubToHost::Ping { nonce: 42 }));
    }

    #[test]
    fn host_catalog_refresh_is_a_correlated_protocol_request() {
        let request_id = RequestId::new();
        let decoded: HubToHost = decode(
            &encode(HubToHost::RefreshHarnessCatalog {
                request_id: request_id.clone(),
            })
            .unwrap(),
        )
        .unwrap();
        assert!(
            matches!(decoded, HubToHost::RefreshHarnessCatalog { request_id: id } if id == request_id)
        );
    }

    #[test]
    fn remote_acp_request_round_trips_with_only_portable_launch_input() {
        let request_id = RequestId::new();
        let cwd = std::env::current_dir().unwrap();
        let decoded: HubToHost = decode(
            &encode(HubToHost::RunAcp {
                request_id: request_id.clone(),
                harness: "fixture".into(),
                harness_options: std::collections::BTreeMap::from([(
                    "model".into(),
                    "fixture-model".into(),
                )]),
                tools: vec!["issue_*".into()],
                canonical_cwd: cwd.clone(),
                prompt: "canonical conversation".into(),
                recovery_prompt: "recovery conversation".into(),
                acp_session_id: Some("session-1".into()),
            })
            .unwrap(),
        )
        .unwrap();
        assert!(
            matches!(decoded, HubToHost::RunAcp { request_id: id, harness, canonical_cwd, .. } if id == request_id && harness == "fixture" && canonical_cwd == cwd)
        );
    }

    #[test]
    fn identifiers_reject_the_wrong_domain_prefix() {
        assert!(
            "hst_0198f73b-9c31-7c01-8000-000000000000"
                .parse::<HostId>()
                .is_ok()
        );
        assert!(
            "ses_0198f73b-9c31-7c01-8000-000000000000"
                .parse::<HostId>()
                .is_err()
        );
    }

    #[test]
    fn agent_config_requires_an_explicit_harness_and_keeps_its_options_opaque() {
        let config = AgentConfig::parse(
            "harness = \"mews\"\n[harness_options]\nmodel = \"openai/gpt-5\"\nreasoning = \"high\"\n",
        )
        .unwrap();
        assert_eq!(config.harness, "mews");
        assert_eq!(config.harness_options["model"], "openai/gpt-5");
        assert_eq!(config.harness_options["reasoning"], "high");
        assert!(AgentConfig::parse("tools = [\"read\"]\n").is_err());
    }

    #[test]
    fn agent_config_rejects_invalid_harness_names_and_options() {
        let invalid_harness = AgentConfig::parse("harness = \"Mews\"\n").unwrap();
        assert!(invalid_harness.validate().is_err());
        let invalid_option =
            AgentConfig::parse("harness = \"mews\"\n[harness_options]\nmodel = \" \"\n").unwrap();
        assert!(invalid_option.validate().is_err());
    }

    #[test]
    fn host_decoder_rejects_malformed_oversized_and_incompatible_frames() {
        assert!(decode::<HubToHost>(b"not-json").is_err());
        assert!(decode::<HubToHost>(&vec![b' '; MAX_HOST_FRAME_BYTES + 1]).is_err());
        let incompatible = serde_json::to_vec(&HostFrame {
            version: HOST_PROTOCOL_VERSION + 1,
            body: HubToHost::Ping { nonce: 7 },
        })
        .unwrap();
        assert!(decode::<HubToHost>(&incompatible).is_err());
    }
}
