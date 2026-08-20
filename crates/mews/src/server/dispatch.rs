use std::{
    fs,
    path::Path,
    sync::{Arc, atomic::Ordering},
};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    app::Mews,
    host::{ConnectedHost, HostControl, HostExecutor},
};
use mews_protocol::{EventActor, EventActorKind, HubRequest, HubResponse, ProtocolError};

use super::{HubRuntime, active_turns, handoff::*, journal};

#[derive(Clone, Copy)]
pub(crate) enum RequestOrigin<'a> {
    Local,
    Host(&'a ConnectedHost),
}

pub(crate) async fn dispatch(
    runtime: &HubRuntime,
    root: &Path,
    origin: RequestOrigin<'_>,
    command_id: &mews_protocol::RequestId,
    request: HubRequest,
) -> Result<(HubResponse, bool)> {
    if let HubRequest::PollEvents {
        consumer_id,
        limit,
        wait_ms,
    } = &request
    {
        let events =
            active_turns::poll_events(runtime, root, consumer_id.clone(), *limit, *wait_ms).await?;
        return Ok((HubResponse::Events(events), false));
    }
    if let HubRequest::PollJournalEntries { query, wait_ms } = &request {
        let events = journal::poll_journal_entries(runtime, root, query.clone(), *wait_ms).await?;
        return Ok((HubResponse::JournalEntries(events), false));
    }
    let is_move = matches!(&request, HubRequest::MoveHub { .. });
    let _operation_guard = if is_move {
        None
    } else {
        Some(runtime.control.handoff_gate.read().await)
    };
    let _handoff_guard = if is_move {
        Some(runtime.control.handoff_gate.write().await)
    } else {
        None
    };
    if runtime.control.moving.load(Ordering::Acquire) && !matches!(request, HubRequest::Status) {
        bail!("Hub is moving; try again after handoff");
    }
    let request = match request {
        HubRequest::PollEvents { .. } | HubRequest::PollJournalEntries { .. } => {
            unreachable!("handled before acquiring the handoff gate")
        }
        HubRequest::StartTurn {
            idempotency_key,
            session_id,
            prompt,
            metadata,
            source,
        } => {
            let turn = active_turns::start_turn(
                runtime,
                root,
                idempotency_key,
                session_id,
                prompt,
                metadata,
                source,
            )
            .await?;
            return Ok((HubResponse::Turn(turn), false));
        }
        request => request,
    };

    let actor = match origin {
        RequestOrigin::Local => EventActor::system(),
        RequestOrigin::Host(host) => EventActor {
            kind: EventActorKind::Host,
            id: Some(host.host_id().to_string()),
        },
    };
    let context = mews_store::CommandContext::new(command_id.to_string(), actor);
    let mut state = Mews::open_connection(root)?;
    let mut mews = state.commands(context);
    if let RequestOrigin::Host(host) = origin {
        mews.ensure_host(host.host_id())?;
    }
    let response = match request {
        HubRequest::Status => HubResponse::Status(mews.installation()?),
        HubRequest::ListAgents => HubResponse::Agents(mews.agents()?),
        HubRequest::InspectAgent {
            slug,
            host_id,
            after_tool,
            tool_limit,
        } => HubResponse::AgentInspection(Box::new(
            inspect_agent(
                runtime,
                &mews,
                root,
                &slug,
                host_id.as_ref(),
                after_tool,
                tool_limit,
            )
            .await?,
        )),
        HubRequest::CreateAgent {
            slug,
            harness,
            harness_options,
        } => HubResponse::Agent(mews.create_agent_with_harness(
            &slug,
            harness.as_deref().unwrap_or(mews_runtime::MEWS_HARNESS),
            harness_options,
        )?),
        HubRequest::RenameAgent { slug, new_slug } => {
            HubResponse::Agent(rename_agent(runtime, &mut mews, &slug, &new_slug).await?)
        }
        HubRequest::ArchiveAgent { slug } => {
            mews.archive_agent(&slug)?;
            HubResponse::Ack
        }
        HubRequest::SetAuth {
            provider,
            credential,
        } => {
            mews.set_auth(&provider, &credential).await?;
            HubResponse::Ack
        }
        HubRequest::RemoveAuth { provider } => {
            mews.remove_auth(&provider).await?;
            HubResponse::Ack
        }
        HubRequest::ListAuth => HubResponse::Auth(mews.auth_statuses().await?),
        HubRequest::ListModels => HubResponse::Models(mews.models().await?),
        HubRequest::RefreshModels => HubResponse::Models(mews.refresh_models().await?),
        HubRequest::GetProviderDefaults => HubResponse::ProviderDefaults(mews.provider_defaults()?),
        HubRequest::SetDefaultModel { model } => {
            mews.set_default_model(&model).await?;
            HubResponse::Ack
        }
        HubRequest::SetDefaultReasoning { reasoning } => {
            mews.set_default_reasoning(reasoning).await?;
            HubResponse::Ack
        }
        HubRequest::SubscribeSession {
            consumer_id,
            session_id,
            consumer_kind,
        } => {
            mews.subscribe_session(&consumer_id, &session_id, consumer_kind)?;
            HubResponse::Ack
        }
        HubRequest::UnsubscribeSession {
            consumer_id,
            session_id,
        } => {
            mews.unsubscribe_session(&consumer_id, &session_id)?;
            HubResponse::Ack
        }
        HubRequest::DeleteConsumer { consumer_id } => {
            mews.delete_consumer(&consumer_id)?;
            HubResponse::Ack
        }
        HubRequest::AcknowledgeEvents {
            consumer_id,
            checkpoint,
        } => {
            mews.acknowledge_events(&consumer_id, checkpoint)?;
            HubResponse::Ack
        }
        HubRequest::QueryJournalEntries { query } => {
            HubResponse::JournalEntries(mews.journal_entries_page(&query)?)
        }
        HubRequest::GetTurn { id } => HubResponse::Turn(mews.turn(&id)?),
        HubRequest::CancelTurn { id } => {
            active_turns::cancel_turn(&runtime.control, root, &id).await;
            mews.cancel_turn(&id)?;
            runtime.control.event_notify.notify_waiters();
            HubResponse::Ack
        }
        HubRequest::ListSessions => HubResponse::Sessions(mews.sessions()?),
        HubRequest::GetSession { id } => HubResponse::Session(mews.session(&id)?),
        HubRequest::GetSessionHistory { id, after, limit } => {
            HubResponse::SessionHistory(mews.session_history_page(&id, after, limit)?)
        }
        HubRequest::GetSessionEntries { id, after, limit } => {
            HubResponse::SessionEntries(mews.session_entries_page(&id, after, limit)?)
        }
        HubRequest::GetSessionModelConfig { id } => {
            let session = mews.session(&id)?;
            HubResponse::SessionModelConfig(mews.session_model_config(&session)?)
        }
        HubRequest::SetSessionModel { id, model } => {
            HubResponse::Session(mews.set_session_model(&id, model.as_deref())?)
        }
        HubRequest::StartSession {
            slug,
            working_directory,
        } => {
            let directory = working_directory.context("session location was not resolved")?;
            HubResponse::Session(
                mews.start_session_on(&slug, &directory, runtime.local_host.as_ref())
                    .await?,
            )
        }
        HubRequest::StartSessionOn {
            slug,
            host_id,
            working_directory,
        } => {
            let installation = mews.installation()?;
            if host_id == installation.hub_host_id {
                HubResponse::Session(
                    mews.start_session_on(&slug, &working_directory, runtime.local_host.as_ref())
                        .await?,
                )
            } else {
                let host = runtime
                    .remote_hosts
                    .lock()
                    .await
                    .get(&host_id)
                    .cloned()
                    .with_context(|| format!("target Host {host_id} is offline"))?;
                HubResponse::Session(
                    mews.start_session_on(&slug, &working_directory, host.as_ref())
                        .await?,
                )
            }
        }
        HubRequest::StartTurn { .. }
        | HubRequest::PollEvents { .. }
        | HubRequest::PollJournalEntries { .. } => {
            unreachable!("handled before locking")
        }
        HubRequest::ListHosts { after, limit } => {
            let installation = mews.installation()?;
            let connected = runtime.remote_hosts.lock().await;
            let hosts = mews
                .hosts()?
                .into_iter()
                .map(|host| crate::HostStatus {
                    connected: host.id == installation.hub_host_id
                        || connected.contains_key(&host.id),
                    host,
                })
                .collect();
            HubResponse::Hosts(host_page(hosts, after.as_ref(), limit)?)
        }
        HubRequest::ListHarnesses => {
            let installation = mews.installation()?;
            let hosts = mews.hosts()?;
            let hub_host = hosts
                .iter()
                .find(|host| host.id == installation.hub_host_id)
                .context("Hub Host is missing from installation")?
                .clone();
            let connected = runtime.remote_hosts.lock().await;
            let local_descriptors = mews_host::HarnessCatalog::discover(Some(root))?.descriptors();
            runtime
                .local_host
                .replace_harness_catalog(local_descriptors.clone());
            let mut catalog = local_descriptors
                .into_iter()
                .map(|descriptor| crate::HostHarnessStatus {
                    host: hub_host.clone(),
                    descriptor,
                })
                .collect::<Vec<_>>();
            for host in hosts
                .into_iter()
                .filter(|host| host.id != installation.hub_host_id)
            {
                if let Some(connection) = connected.get(&host.id) {
                    catalog.extend(connection.harness_catalog().into_iter().map(|descriptor| {
                        crate::HostHarnessStatus {
                            host: host.clone(),
                            descriptor,
                        }
                    }));
                }
            }
            catalog.sort_by(|left, right| {
                left.descriptor
                    .name
                    .cmp(&right.descriptor.name)
                    .then_with(|| left.host.name.cmp(&right.host.name))
            });
            HubResponse::Harnesses(catalog)
        }
        HubRequest::RefreshHarnesses => {
            let installation = mews.installation()?;
            let hosts = mews.hosts()?;
            let hub_host = hosts
                .iter()
                .find(|host| host.id == installation.hub_host_id)
                .context("Hub Host is missing from installation")?
                .clone();
            let connected = runtime.remote_hosts.lock().await;
            let local_descriptors = mews_host::HarnessCatalog::discover(Some(root))?.descriptors();
            runtime
                .local_host
                .replace_harness_catalog(local_descriptors.clone());
            let mut catalog = local_descriptors
                .into_iter()
                .map(|descriptor| crate::HostHarnessStatus {
                    host: hub_host.clone(),
                    descriptor,
                })
                .collect::<Vec<_>>();
            for host in hosts
                .into_iter()
                .filter(|host| host.id != installation.hub_host_id)
            {
                let connection = connected
                    .get(&host.id)
                    .with_context(|| format!("Host {} is offline", host.name))?;
                let descriptors = connection.refresh_harness_catalog().await?;
                catalog.extend(descriptors.into_iter().map(|descriptor| {
                    crate::HostHarnessStatus {
                        host: host.clone(),
                        descriptor,
                    }
                }));
            }
            catalog.sort_by(|left, right| {
                left.descriptor
                    .name
                    .cmp(&right.descriptor.name)
                    .then_with(|| left.host.name.cmp(&right.host.name))
            });
            HubResponse::Harnesses(catalog)
        }
        HubRequest::RemoveHost { id } => {
            mews.remove_host(&id)?;
            runtime.remote_hosts.lock().await.remove(&id);
            HubResponse::Ack
        }
        HubRequest::CreateHostInvitation { relay_url } => {
            let offer = mews.create_invitation(relay_url.as_deref())?;
            let encoded = offer.encode()?;
            let root = root.to_path_buf();
            let remote_hosts = Arc::clone(&runtime.remote_hosts);
            let control = runtime.control.clone();
            let local_host = Arc::clone(&runtime.local_host);
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            tokio::task::spawn_local(async move {
                if let Err(error) = crate::enrollment::join::accept_join_ready(
                    &root,
                    offer,
                    ready_tx,
                    remote_hosts,
                    control,
                    local_host,
                )
                .await
                {
                    eprintln!("Host enrollment listener stopped: {error:#}");
                }
            });
            ready_rx
                .await
                .context("Host enrollment listener stopped")??;
            HubResponse::HostInvitation(encoded)
        }
        HubRequest::MoveHub { host } => {
            let target = mews
                .hosts()?
                .into_iter()
                .find(|candidate| candidate.name == host || candidate.id.to_string() == host)
                .context("target Host not found")?;
            let connected = runtime.remote_hosts.lock().await.get(&target.id).cloned();
            let Some(connected) = connected else {
                bail!("target Host is not connected");
            };
            connected.configure_relay(true, None).await?;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            write_move_phase(root, "preparing")?;
            let mut demoted = mews.demoted_host_state(&target.id)?;
            runtime
                .control
                .moving
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| anyhow::anyhow!("Hub move already in progress"))?;
            let snapshot = match mews.begin_hub_move(&target.id) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    runtime.control.moving.store(false, Ordering::Release);
                    let _ = connected.configure_relay(false, None).await;
                    let _ = fs::remove_file(root.join("hub.json"));
                    let _ = fs::remove_file(root.join("hub-move.phase"));
                    return Err(error);
                }
            };
            let relay_urls = mews.relay_candidates()?;
            demoted.relay_urls = relay_urls.clone();
            demoted.accepted.relay_urls = relay_urls.clone();
            write_demoted_state(root, &demoted)?;
            let connected_hosts: Vec<_> = runtime
                .remote_hosts
                .lock()
                .await
                .values()
                .cloned()
                .collect();
            let enrolled_remote_hosts = mews.hosts()?.len().saturating_sub(1);
            let mut acknowledged = 0;
            for host in &connected_hosts {
                if host
                    .update_relay_candidates(relay_urls.clone())
                    .await
                    .is_ok()
                {
                    acknowledged += 1;
                }
            }
            let transfer = transfer_hub_snapshot(&connected, &snapshot).await;
            let _ = fs::remove_file(&snapshot.database_path);
            if let Err(error) = transfer {
                let rollback = mews.rollback_hub_move(&snapshot);
                let _ = fs::remove_file(root.join("hub.json"));
                let _ = fs::remove_file(root.join("hub-move.phase"));
                let _ = fs::remove_file(root.join("hub-move-recovery.json"));
                runtime.control.moving.store(false, Ordering::Release);
                let _ = connected.configure_relay(false, None).await;
                rollback.context("Hub transfer failed and rollback also failed")?;
                return Err(error);
            }
            write_move_recovery(root, &snapshot)?;
            if let Err(error) = write_move_phase(root, "activating") {
                let rollback = mews.rollback_hub_move(&snapshot);
                let _ = fs::remove_file(root.join("hub.json"));
                let _ = fs::remove_file(root.join("hub-move.phase"));
                let _ = fs::remove_file(root.join("hub-move-recovery.json"));
                runtime.control.moving.store(false, Ordering::Release);
                let _ = connected.configure_relay(false, None).await;
                rollback.context("phase journal failed and rollback also failed")?;
                return Err(error);
            }
            let grace = if acknowledged == enrolled_remote_hosts {
                chrono::Duration::minutes(10)
            } else {
                chrono::Duration::days(10)
            };
            let mut relay = crate::relay_supervisor::read(root)?;
            relay.role = crate::relay_supervisor::RelayRole::Retiring {
                stop_at: chrono::Utc::now() + grace,
            };
            crate::relay_supervisor::write(root, &relay)?;
            if let Err(error) = connected.arm_hub_transfer(&snapshot.move_nonce).await {
                eprintln!(
                    "Hub target did not acknowledge arming; retrying while fenced: {error:#}"
                );
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let candidate = runtime.remote_hosts.lock().await.get(&target.id).cloned();
                    if let Some(candidate) = candidate
                        && candidate
                            .arm_hub_transfer(&snapshot.move_nonce)
                            .await
                            .is_ok()
                    {
                        let response = match candidate.activate_hub_transfer().await {
                            Ok(()) => HubResponse::Ack,
                            Err(error) => HubResponse::Error(ProtocolError::internal(format!(
                                "Hub activation outcome is uncertain; target is armed and old Hub was safely demoted: {error:#}"
                            ))),
                        };
                        runtime.control.journal_notify.notify_waiters();
                        return Ok((response, true));
                    }
                }
            }
            let response = match connected.activate_hub_transfer().await {
                Ok(()) => HubResponse::Ack,
                Err(error) => HubResponse::Error(ProtocolError::internal(format!(
                    "Hub activation outcome is uncertain; the old Hub was safely demoted: {error:#}"
                ))),
            };
            runtime.control.journal_notify.notify_waiters();
            return Ok((response, true));
        }
        HubRequest::Shutdown => return Ok((HubResponse::Ack, true)),
    };
    // Synchronous application commands have committed before reaching here.
    // Waking on reads is harmless and keeps this boundary deliberately simple;
    // long polls always recheck the exclusive cursor before sleeping.
    runtime.control.journal_notify.notify_waiters();
    Ok((response, false))
}

async fn inspect_agent(
    runtime: &HubRuntime,
    mews: &Mews,
    root: &std::path::Path,
    slug: &str,
    host_id: Option<&mews_protocol::HostId>,
    after_tool: Option<mews_protocol::AgentToolCursor>,
    tool_limit: u16,
) -> Result<mews_protocol::AgentInspection> {
    let agent = mews
        .agents()?
        .into_iter()
        .find(|agent| agent.slug == slug && !agent.archived)
        .with_context(|| format!("Agent {slug:?} does not exist"))?;
    let revision = mews.agent_revision(&agent)?;
    let config = crate::AgentConfig::parse(&revision.config_toml)?;
    let selected_host = if let Some(host_id) = host_id {
        Some(
            mews.hosts()?
                .into_iter()
                .find(|host| &host.id == host_id)
                .with_context(|| format!("Host {host_id} does not exist"))?,
        )
    } else {
        None
    };
    let mut all_tools = Vec::new();
    let mut is_connected = false;
    let mut harness = None;
    let mut snapshot = None;
    let mut skills_present = None;
    if let Some(host) = selected_host.as_ref() {
        let installation = mews.installation()?;
        let connected_hosts = runtime.remote_hosts.lock().await;
        let is_local = host.id == installation.hub_host_id;
        let connection = if is_local {
            Some(runtime.local_host.as_ref())
        } else {
            connected_hosts.get(&host.id).map(AsRef::as_ref)
        };
        if let Some(connection) = connection {
            is_connected = true;
            harness = connection.harness_descriptor(&config.harness);
            snapshot = Some(connection.agent_capabilities().tools());
            all_tools = inspected_tools(
                &agent.id,
                &config,
                snapshot.as_ref().expect("tool snapshot was set"),
                harness.as_ref(),
            )?;
            if config.harness != mews_runtime::MEWS_HARNESS && is_local {
                skills_present = Some(
                    !mews_host::resources::snapshot_agent_skills(root, &agent.slug)?.is_empty(),
                );
            }
        }
    }
    let skill_tools = selected_host
        .as_ref()
        .map(|_| acp_skill_tools(&config, harness.as_ref(), skills_present));
    let snapshot_hash = agent_inspection_snapshot_hash(AgentInspectionSnapshotBinding {
        agent: &agent,
        revision: &revision,
        config: &config,
        host: selected_host.as_ref(),
        connected: is_connected,
        harness: harness.as_ref(),
        tool_catalog: snapshot.as_ref(),
        acp_skill_tools: skill_tools.as_ref(),
        tools: &all_tools,
    })?;
    let host = selected_host.map(|host| mews_protocol::AgentHostInspection {
        host,
        connected: is_connected,
        harness_native_authority: harness_native_authority(&config, harness.as_ref()),
        acp_skill_tools: skill_tools.expect("selected Host has a skill-tool state"),
        harness: harness.map(compact_harness),
        tool_catalog_generation: snapshot.as_ref().map(|snapshot| snapshot.generation),
        tools: mews_protocol::AgentToolInspectionPage {
            tools: Vec::new(),
            next: None,
        },
    });
    let inspection = mews_protocol::AgentInspection {
        agent,
        revision_hash: revision.content_hash,
        author_host_id: revision.author_host_id,
        config,
        host,
    };
    paginate_agent_inspection(inspection, all_tools, snapshot_hash, after_tool, tool_limit)
}

#[derive(Serialize)]
struct AgentInspectionSnapshotBinding<'a> {
    agent: &'a mews_protocol::Agent,
    revision: &'a mews_protocol::AgentRevision,
    config: &'a mews_protocol::AgentConfig,
    host: Option<&'a mews_protocol::Host>,
    connected: bool,
    harness: Option<&'a mews_protocol::HarnessDescriptor>,
    tool_catalog: Option<&'a mews_protocol::ToolCatalogSnapshot>,
    acp_skill_tools: Option<&'a mews_protocol::AcpSkillToolsInspection>,
    tools: &'a [mews_protocol::AgentToolInspection],
}

fn agent_inspection_snapshot_hash(binding: AgentInspectionSnapshotBinding<'_>) -> Result<[u8; 32]> {
    Ok(Sha256::digest(serde_json::to_vec(&binding)?).into())
}

fn host_page(
    mut hosts: Vec<mews_protocol::HostStatus>,
    after: Option<&mews_protocol::HostId>,
    limit: u16,
) -> Result<mews_protocol::HostPage> {
    if limit == 0 {
        bail!("Host page limit must be greater than zero");
    }
    hosts.sort_by(|left, right| left.host.id.as_str().cmp(right.host.id.as_str()));
    let remaining = hosts
        .into_iter()
        .filter(|status| after.is_none_or(|after| status.host.id.as_str() > after.as_str()))
        .collect::<Vec<_>>();
    let maximum = remaining.len().min(usize::from(limit));
    let mut low = 0;
    let mut high = maximum;
    while low < high {
        let count = low + (high - low).div_ceil(2);
        let response = HubResponse::Hosts(mews_protocol::HostPage {
            hosts: remaining[..count].to_vec(),
            next: (count < remaining.len()).then(|| remaining[count - 1].host.id.clone()),
        });
        if hub_response_fits(&response)? {
            low = count;
        } else {
            high = count - 1;
        }
    }
    if low == 0 && !remaining.is_empty() {
        bail!("one Host status exceeds the Hub frame limit");
    }
    let page = remaining[..low].to_vec();
    let next = if low < remaining.len() {
        page.last().map(|status| status.host.id.clone())
    } else {
        None
    };
    Ok(mews_protocol::HostPage { hosts: page, next })
}

fn paginate_agent_inspection(
    mut inspection: mews_protocol::AgentInspection,
    all_tools: Vec<mews_protocol::AgentToolInspection>,
    snapshot_hash: [u8; 32],
    after_tool: Option<mews_protocol::AgentToolCursor>,
    tool_limit: u16,
) -> Result<mews_protocol::AgentInspection> {
    if tool_limit == 0 {
        bail!("tool page limit must be greater than zero");
    }
    if after_tool.is_some_and(|cursor| cursor.snapshot_hash != snapshot_hash) {
        bail!("inspection snapshot changed while tools were paged; restart inspection");
    }
    let start = usize::try_from(after_tool.map_or(0, |cursor| cursor.offset))
        .context("tool cursor is too large")?;
    if start > all_tools.len() {
        bail!("tool cursor is outside the catalog");
    }
    let Some(host) = inspection.host.as_mut() else {
        return Ok(inspection);
    };
    host.tools = mews_protocol::AgentToolInspectionPage {
        tools: Vec::new(),
        next: None,
    };
    if !hub_response_fits(&HubResponse::AgentInspection(Box::new(inspection.clone())))? {
        bail!("Agent inspection metadata exceeds the Hub frame limit");
    }

    let maximum = all_tools.len().min(start + usize::from(tool_limit));
    let mut low = start;
    let mut high = maximum;
    while low < high {
        let end = low + (high - low).div_ceil(2);
        let mut candidate = inspection.clone();
        let tools = &mut candidate
            .host
            .as_mut()
            .expect("Host presence was checked")
            .tools;
        tools.tools = all_tools[start..end].to_vec();
        tools.next = if end < all_tools.len() {
            Some(mews_protocol::AgentToolCursor {
                snapshot_hash,
                offset: u32::try_from(end).context("tool catalog has too many entries")?,
            })
        } else {
            None
        };
        if hub_response_fits(&HubResponse::AgentInspection(Box::new(candidate)))? {
            low = end;
        } else {
            high = end - 1;
        }
    }
    if low == start && start < all_tools.len() {
        bail!("one inspected tool exceeds the Hub frame limit");
    }
    let end = low;
    let tools = &mut inspection
        .host
        .as_mut()
        .expect("Host presence was checked")
        .tools;
    tools.tools = all_tools[start..end].to_vec();
    tools.next = if end < all_tools.len() {
        Some(mews_protocol::AgentToolCursor {
            snapshot_hash,
            offset: u32::try_from(end).context("tool catalog has too many entries")?,
        })
    } else {
        None
    };
    Ok(inspection)
}

fn hub_response_fits(response: &HubResponse) -> Result<bool> {
    let frame = mews_protocol::Frame::with_request_id(response, mews_protocol::RequestId::new());
    Ok(serde_json::to_vec(&frame)?.len() <= mews_protocol::MAX_HUB_FRAME_BYTES)
}

fn acp_skill_tools(
    config: &mews_protocol::AgentConfig,
    harness: Option<&mews_protocol::HarnessDescriptor>,
    skills_present: Option<bool>,
) -> mews_protocol::AcpSkillToolsInspection {
    use mews_protocol::AcpSkillToolsState;

    if config.harness == mews_runtime::MEWS_HARNESS {
        return mews_protocol::AcpSkillToolsInspection {
            names: Vec::new(),
            state: AcpSkillToolsState::NotApplicable,
        };
    }
    if skills_present == Some(false) {
        return mews_protocol::AcpSkillToolsInspection {
            names: Vec::new(),
            state: AcpSkillToolsState::NoneKnown,
        };
    }
    let state = if harness.is_none_or(|harness| !harness.availability.ready()) {
        AcpSkillToolsState::HarnessUnavailable
    } else if !harness.is_some_and(|harness| harness.supports_http_mcp) {
        AcpSkillToolsState::UnsupportedTransport
    } else {
        AcpSkillToolsState::Conditional
    };
    mews_protocol::AcpSkillToolsInspection {
        names: mews_protocol::ACP_SKILL_TOOL_NAMES
            .into_iter()
            .map(str::to_owned)
            .collect(),
        state,
    }
}

fn compact_harness(
    descriptor: mews_protocol::HarnessDescriptor,
) -> mews_protocol::AgentHarnessInspection {
    mews_protocol::AgentHarnessInspection {
        name: descriptor.name,
        protocol: descriptor.protocol,
        availability: descriptor.availability,
        supports_http_mcp: descriptor.supports_http_mcp,
    }
}

fn harness_native_authority(
    config: &mews_protocol::AgentConfig,
    harness: Option<&mews_protocol::HarnessDescriptor>,
) -> mews_protocol::HarnessNativeAuthority {
    if config.harness == mews_runtime::MEWS_HARNESS {
        mews_protocol::HarnessNativeAuthority::NotApplicable
    } else if harness.is_some_and(|harness| !harness.native_tools.is_empty()) {
        mews_protocol::HarnessNativeAuthority::KnownUncontrolled
    } else {
        mews_protocol::HarnessNativeAuthority::UnknownUncontrolled
    }
}

fn inspected_tools(
    agent_id: &mews_protocol::AgentId,
    config: &mews_protocol::AgentConfig,
    snapshot: &mews_protocol::ToolCatalogSnapshot,
    harness: Option<&mews_protocol::HarnessDescriptor>,
) -> Result<Vec<mews_protocol::AgentToolInspection>> {
    let allowed = |name: &str| {
        config
            .tools
            .iter()
            .any(|pattern| mews_agent::tool_allowed(pattern, name))
    };
    let mut tools = Vec::new();
    if config.harness == mews_runtime::MEWS_HARNESS {
        tools.extend(snapshot.tools.iter().filter_map(|tool| {
            let source = match &tool.agent_id {
                None => mews_protocol::AgentToolSource::MewsNative,
                Some(owner) if owner == agent_id => mews_protocol::AgentToolSource::AgentExtension,
                Some(_) => return None,
            };
            Some(mews_protocol::AgentToolInspection {
                name: tool.name.clone(),
                source,
                allowlist_match: allowed(&tool.name),
                exposure: if allowed(&tool.name) {
                    mews_protocol::AgentToolExposure::Exposed
                } else {
                    mews_protocol::AgentToolExposure::ExcludedByAllowlist
                },
            })
        }));
    } else {
        if snapshot.tools.iter().any(|tool| {
            tool.agent_id.as_ref() == Some(agent_id)
                && mews_protocol::is_reserved_acp_skill_tool(&tool.name)
        }) {
            bail!("Host tool catalog contains a reserved ACP skill-tool name");
        }
        if let Some(harness) = harness {
            tools.extend(harness.native_tools.iter().map(|name| {
                mews_protocol::AgentToolInspection {
                    name: name.clone(),
                    source: mews_protocol::AgentToolSource::HarnessNative,
                    allowlist_match: allowed(name),
                    exposure: mews_protocol::AgentToolExposure::HarnessControlled,
                }
            }));
        }
        let extension_exposure = |name: &str| {
            if !allowed(name) {
                mews_protocol::AgentToolExposure::ExcludedByAllowlist
            } else if harness.is_none_or(|harness| !harness.availability.ready()) {
                mews_protocol::AgentToolExposure::HarnessUnavailable
            } else if !harness.is_some_and(|harness| harness.supports_http_mcp) {
                mews_protocol::AgentToolExposure::UnsupportedTransport
            } else {
                mews_protocol::AgentToolExposure::Exposed
            }
        };
        tools.extend(
            snapshot
                .tools
                .iter()
                .filter(|tool| tool.agent_id.as_ref() == Some(agent_id))
                .map(|tool| mews_protocol::AgentToolInspection {
                    name: tool.name.clone(),
                    source: mews_protocol::AgentToolSource::AgentExtension,
                    allowlist_match: allowed(&tool.name),
                    exposure: extension_exposure(&tool.name),
                }),
        );
    }
    tools.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(tools)
}

async fn rename_agent(
    runtime: &HubRuntime,
    mews: &mut crate::app::MewsCommands<'_>,
    slug: &str,
    new_slug: &str,
) -> Result<crate::Agent> {
    let installation = mews.installation()?;
    let remote_ids = mews
        .hosts()?
        .into_iter()
        .filter(|host| host.id != installation.hub_host_id)
        .map(|host| host.id)
        .collect::<Vec<_>>();
    let connected = runtime.remote_hosts.lock().await;
    let remote = remote_ids
        .iter()
        .map(|id| {
            connected
                .get(id)
                .cloned()
                .with_context(|| format!("Host {id} must be online before renaming an agent"))
        })
        .collect::<Result<Vec<_>>>()?;
    drop(connected);

    // A response can be lost after the durable rename. Replay the receipt and
    // finish any pending local move before trying to synchronize the old slug.
    if let Some(renamed) = mews.replay_agent_rename(slug, new_slug)? {
        let revision = mews.agent_revision(&renamed)?;
        for host in &remote {
            if let Some(replica) = host.agent_replica(new_slug).await? {
                if !replica_matches_revision(&replica, &revision) {
                    bail!(
                        "Host {} has an unsynchronized renamed agent replica",
                        host.host_id()
                    );
                }
                continue;
            }
            let previous = host.agent_replica(slug).await?;
            if let Some(replica) = &previous
                && !replica_matches_revision(replica, &revision)
            {
                bail!(
                    "Host {} has an unsynchronized agent replica; synchronize it before renaming",
                    host.host_id()
                );
            }
            host.synchronize_agent(&renamed, &revision, previous.as_ref(), Some(slug))
                .await?;
        }
        return Ok(renamed);
    }

    let current_agent = mews.synchronize_agent(slug)?;
    let current = mews.agent_revision(&current_agent)?;
    let mut replicas = Vec::with_capacity(remote.len());
    for host in &remote {
        let replica = host.agent_replica(slug).await?;
        if let Some(replica) = &replica
            && (replica.revision != current.revision
                || replica.soul.trim_end() != current.soul.trim_end()
                || replica.config_toml != current.config_toml)
        {
            bail!(
                "Host {} has an unsynchronized agent replica; synchronize it before renaming",
                host.host_id()
            );
        }
        replicas.push(replica);
    }

    let renamed = mews.rename_agent(slug, new_slug)?;
    let revision = mews.agent_revision(&renamed)?;
    for (host, expected) in remote.iter().zip(&replicas) {
        host.synchronize_agent(&renamed, &revision, expected.as_ref(), Some(slug))
            .await?;
    }
    Ok(renamed)
}

fn replica_matches_revision(
    replica: &mews_protocol::AgentReplica,
    revision: &crate::AgentRevision,
) -> bool {
    replica.revision == revision.revision
        && replica.soul.trim_end() == revision.soul.trim_end()
        && replica.config_toml == revision.config_toml
}

#[cfg(test)]
mod inspection_tests {
    use std::collections::BTreeMap;

    use mews_protocol::{
        AcpSkillToolsInspection, AcpSkillToolsState, Agent, AgentConfig, AgentHostInspection,
        AgentInspection, AgentRevision, AgentToolExposure, AgentToolInspection,
        AgentToolInspectionPage, AgentToolSource, HarnessAvailability, HarnessDescriptor,
        HarnessNativeAuthority, HarnessProtocol, HarnessReadiness, Host, HostStatus, HubResponse,
        ToolCatalogSnapshot, ToolDefinition, ToolExecutionMode,
    };
    use serde_json::json;

    use super::{
        AgentInspectionSnapshotBinding, acp_skill_tools, agent_inspection_snapshot_hash,
        harness_native_authority, host_page, inspected_tools, paginate_agent_inspection,
    };

    fn acp_harness(supports_http_mcp: bool) -> HarnessDescriptor {
        HarnessDescriptor {
            name: "codex".into(),
            protocol: HarnessProtocol::Acp,
            definition_hash: "test".into(),
            availability: HarnessAvailability {
                runtime: HarnessReadiness::Ready,
                adapter: HarnessReadiness::Ready,
                authentication: HarnessReadiness::Ready,
                catalog: HarnessReadiness::Ready,
                detail: None,
            },
            executable_version: None,
            native_tools: Vec::new(),
            modes: Vec::new(),
            supports_http_mcp,
            supports_continuation: false,
            models: Vec::new(),
            config_options: Vec::new(),
            probed_at: None,
        }
    }

    #[test]
    fn inspection_uses_runtime_allowlist_and_agent_ownership() {
        let agent_id = mews_protocol::AgentId::new();
        let other_id = mews_protocol::AgentId::new();
        let definition = |name: &str, owner| ToolDefinition {
            name: name.into(),
            description: name.into(),
            schema: json!({"type": "object"}),
            agent_id: owner,
        };
        let snapshot = ToolCatalogSnapshot {
            generation: 9,
            tools: vec![
                definition("read", None),
                definition("bash", None),
                definition("git_status", Some(agent_id.clone())),
                definition("private", Some(other_id)),
            ],
        };
        let config = AgentConfig {
            harness: mews_runtime::MEWS_HARNESS.into(),
            harness_options: BTreeMap::new(),
            tools: vec!["read".into(), "git_*".into()],
            tool_execution: ToolExecutionMode::Parallel,
        };

        let tools = inspected_tools(&agent_id, &config, &snapshot, None).unwrap();

        assert_eq!(
            tools
                .iter()
                .map(|tool| {
                    (
                        tool.name.as_str(),
                        tool.source,
                        tool.allowlist_match,
                        tool.exposure,
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (
                    "bash",
                    AgentToolSource::MewsNative,
                    false,
                    AgentToolExposure::ExcludedByAllowlist,
                ),
                (
                    "git_status",
                    AgentToolSource::AgentExtension,
                    true,
                    AgentToolExposure::Exposed,
                ),
                (
                    "read",
                    AgentToolSource::MewsNative,
                    true,
                    AgentToolExposure::Exposed,
                ),
            ]
        );
        assert_eq!(
            harness_native_authority(&config, None),
            HarnessNativeAuthority::NotApplicable
        );
    }

    #[test]
    fn acp_extension_exposure_requires_a_ready_http_mcp_harness() {
        let agent_id = mews_protocol::AgentId::new();
        let snapshot = ToolCatalogSnapshot {
            generation: 3,
            tools: vec![ToolDefinition {
                name: "lookup".into(),
                description: "Lookup".into(),
                schema: json!({"type":"object"}),
                agent_id: Some(agent_id.clone()),
            }],
        };
        let config = AgentConfig {
            harness: "codex".into(),
            harness_options: BTreeMap::new(),
            tools: vec!["lookup".into()],
            tool_execution: ToolExecutionMode::Parallel,
        };

        assert_eq!(
            inspected_tools(&agent_id, &config, &snapshot, None).unwrap()[0].exposure,
            AgentToolExposure::HarnessUnavailable
        );
        assert_eq!(
            inspected_tools(&agent_id, &config, &snapshot, Some(&acp_harness(false))).unwrap()[0]
                .exposure,
            AgentToolExposure::UnsupportedTransport
        );
        assert_eq!(
            inspected_tools(&agent_id, &config, &snapshot, Some(&acp_harness(true))).unwrap()[0]
                .exposure,
            AgentToolExposure::Exposed
        );
        assert_eq!(
            harness_native_authority(&config, Some(&acp_harness(true))),
            HarnessNativeAuthority::UnknownUncontrolled
        );

        let mut known = acp_harness(true);
        known.native_tools.push("shell".into());
        let tools = inspected_tools(&agent_id, &config, &snapshot, Some(&known)).unwrap();
        assert_eq!(
            harness_native_authority(&config, Some(&known)),
            HarnessNativeAuthority::KnownUncontrolled
        );
        assert!(tools.iter().any(|tool| {
            tool.name == "shell" && tool.exposure == AgentToolExposure::HarnessControlled
        }));

        assert_eq!(
            acp_skill_tools(&config, Some(&acp_harness(true)), Some(false)).state,
            AcpSkillToolsState::NoneKnown
        );
        assert_eq!(
            acp_skill_tools(&config, Some(&acp_harness(true)), Some(true)).state,
            AcpSkillToolsState::Conditional
        );
        assert_eq!(
            acp_skill_tools(&config, Some(&acp_harness(false)), None).state,
            AcpSkillToolsState::UnsupportedTransport
        );
        assert_eq!(
            acp_skill_tools(&config, None, None).state,
            AcpSkillToolsState::HarnessUnavailable
        );
        for name in mews_protocol::ACP_SKILL_TOOL_NAMES {
            let mut invalid = snapshot.clone();
            invalid.tools[0].name = name.into();
            assert!(
                inspected_tools(&agent_id, &config, &invalid, Some(&acp_harness(true)))
                    .unwrap_err()
                    .to_string()
                    .contains("reserved ACP skill-tool name")
            );
        }
    }

    fn host(name: String) -> Host {
        Host {
            id: mews_protocol::HostId::new(),
            name,
            public_key: "p".repeat(64),
            noise_public_key: "n".repeat(64),
            relay_url: None,
            created_at: chrono::Utc::now(),
        }
    }

    fn inspection() -> AgentInspection {
        AgentInspection {
            agent: Agent {
                id: mews_protocol::AgentId::new(),
                slug: "coder".into(),
                current_revision: 1,
                archived: false,
                created_at: chrono::Utc::now(),
            },
            revision_hash: "a".repeat(64),
            author_host_id: mews_protocol::HostId::new(),
            config: AgentConfig {
                harness: "codex".into(),
                harness_options: BTreeMap::new(),
                tools: vec!["*".into()],
                tool_execution: ToolExecutionMode::Parallel,
            },
            host: Some(AgentHostInspection {
                host: host("test".into()),
                connected: true,
                harness: None,
                harness_native_authority: HarnessNativeAuthority::UnknownUncontrolled,
                acp_skill_tools: AcpSkillToolsInspection {
                    names: vec!["mews_list_skills".into(), "mews_read_skill".into()],
                    state: AcpSkillToolsState::Conditional,
                },
                tool_catalog_generation: Some(7),
                tools: AgentToolInspectionPage {
                    tools: Vec::new(),
                    next: None,
                },
            }),
        }
    }

    #[test]
    fn host_discovery_pages_are_complete_and_frame_bounded() {
        let statuses = (0..6_000)
            .map(|index| HostStatus {
                host: host(format!("host-{index:04}-{}", "x".repeat(96))),
                connected: index % 2 == 0,
            })
            .collect::<Vec<_>>();
        let mut expected = statuses
            .iter()
            .map(|status| status.host.id.clone())
            .collect::<Vec<_>>();
        expected.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let mut after = None;
        let mut actual = Vec::new();
        let mut pages = 0;
        loop {
            let page = host_page(statuses.clone(), after.as_ref(), u16::MAX).unwrap();
            let response = HubResponse::Hosts(page.clone());
            let frame =
                mews_protocol::Frame::with_request_id(response, mews_protocol::RequestId::new());
            assert!(mews_protocol::encode_hub_frame(&frame).is_ok());
            actual.extend(page.hosts.into_iter().map(|status| status.host.id));
            pages += 1;
            let Some(next) = page.next else {
                break;
            };
            after = Some(next);
        }
        assert!(pages >= 2);
        assert_eq!(actual, expected);
    }

    #[test]
    fn agent_tool_pages_are_complete_and_frame_bounded() {
        let tools = (0..10_000)
            .map(|index| AgentToolInspection {
                name: format!("tool_{index:05}_{}", "x".repeat(160)),
                source: AgentToolSource::AgentExtension,
                allowlist_match: true,
                exposure: AgentToolExposure::Exposed,
            })
            .collect::<Vec<_>>();
        let mut after = None;
        let mut actual = Vec::new();
        let mut pages = 0;
        let snapshot_hash = [7; 32];
        loop {
            let page = paginate_agent_inspection(
                inspection(),
                tools.clone(),
                snapshot_hash,
                after,
                u16::MAX,
            )
            .unwrap();
            let frame = mews_protocol::Frame::with_request_id(
                HubResponse::AgentInspection(Box::new(page.clone())),
                mews_protocol::RequestId::new(),
            );
            assert!(mews_protocol::encode_hub_frame(&frame).is_ok());
            let tool_page = page.host.unwrap().tools;
            actual.extend(tool_page.tools.into_iter().map(|tool| tool.name));
            pages += 1;
            let Some(next) = tool_page.next else {
                break;
            };
            after = Some(next);
        }
        assert!(pages >= 2);
        assert_eq!(
            actual,
            tools.into_iter().map(|tool| tool.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn agent_tool_cursor_rejects_every_changed_snapshot_input() {
        let inspection = inspection();
        let agent = &inspection.agent;
        let revision = AgentRevision {
            agent_id: agent.id.clone(),
            revision: agent.current_revision,
            soul: "soul".into(),
            config_toml: "harness = 'codex'".into(),
            content_hash: inspection.revision_hash.clone(),
            author_host_id: inspection.author_host_id.clone(),
            created_at: agent.created_at,
        };
        let harness = acp_harness(true);
        let catalog = ToolCatalogSnapshot {
            generation: 7,
            tools: ["one", "two"]
                .into_iter()
                .map(|name| ToolDefinition {
                    name: name.into(),
                    description: name.into(),
                    schema: json!({"type":"object"}),
                    agent_id: Some(agent.id.clone()),
                })
                .collect(),
        };
        let skill_tools = inspection.host.as_ref().unwrap().acp_skill_tools.clone();
        let tools =
            inspected_tools(&agent.id, &inspection.config, &catalog, Some(&harness)).unwrap();
        let hash = |agent: &Agent,
                    revision: &AgentRevision,
                    config: &AgentConfig,
                    connected: bool,
                    harness: Option<&HarnessDescriptor>,
                    catalog: Option<&ToolCatalogSnapshot>,
                    skill_tools: &AcpSkillToolsInspection,
                    tools: &[AgentToolInspection]| {
            agent_inspection_snapshot_hash(AgentInspectionSnapshotBinding {
                agent,
                revision,
                config,
                host: inspection.host.as_ref().map(|host| &host.host),
                connected,
                harness,
                tool_catalog: catalog,
                acp_skill_tools: Some(skill_tools),
                tools,
            })
            .unwrap()
        };
        let snapshot_hash = hash(
            agent,
            &revision,
            &inspection.config,
            true,
            Some(&harness),
            Some(&catalog),
            &skill_tools,
            &tools,
        );
        let first =
            paginate_agent_inspection(inspection.clone(), tools.clone(), snapshot_hash, None, 1)
                .unwrap();
        let cursor = first.host.unwrap().tools.next.unwrap();

        let mut native_tools = harness.clone();
        native_tools.native_tools.push("shell".into());
        let mut unavailable = harness.clone();
        unavailable.availability.runtime = HarnessReadiness::Missing;
        let mut edited_agent = agent.clone();
        edited_agent.current_revision += 1;
        let mut edited_revision = revision.clone();
        edited_revision.revision += 1;
        edited_revision.config_toml = "harness = 'codex'\ntools = ['one']".into();
        let mut edited_config = inspection.config.clone();
        edited_config.tools = vec!["one".into()];
        let edited_tools =
            inspected_tools(&agent.id, &edited_config, &catalog, Some(&harness)).unwrap();
        let mut changed_catalog = catalog.clone();
        changed_catalog.tools[0].description = "changed without a generation bump".into();
        let mut changed_skill_tools = skill_tools.clone();
        changed_skill_tools.state = AcpSkillToolsState::UnsupportedTransport;
        let mut changed_rows = tools.clone();
        changed_rows.reverse();

        let changed_hashes = [
            (
                "Harness native tools",
                hash(
                    agent,
                    &revision,
                    &inspection.config,
                    true,
                    Some(&native_tools),
                    Some(&catalog),
                    &skill_tools,
                    &tools,
                ),
            ),
            (
                "Harness availability",
                hash(
                    agent,
                    &revision,
                    &inspection.config,
                    true,
                    Some(&unavailable),
                    Some(&catalog),
                    &skill_tools,
                    &tools,
                ),
            ),
            (
                "Agent revision and config",
                hash(
                    &edited_agent,
                    &edited_revision,
                    &edited_config,
                    true,
                    Some(&harness),
                    Some(&catalog),
                    &skill_tools,
                    &edited_tools,
                ),
            ),
            (
                "tool catalog definitions",
                hash(
                    agent,
                    &revision,
                    &inspection.config,
                    true,
                    Some(&harness),
                    Some(&changed_catalog),
                    &skill_tools,
                    &tools,
                ),
            ),
            (
                "ACP skill-tool state",
                hash(
                    agent,
                    &revision,
                    &inspection.config,
                    true,
                    Some(&harness),
                    Some(&catalog),
                    &changed_skill_tools,
                    &tools,
                ),
            ),
            (
                "ordered inspected rows",
                hash(
                    agent,
                    &revision,
                    &inspection.config,
                    true,
                    Some(&harness),
                    Some(&catalog),
                    &skill_tools,
                    &changed_rows,
                ),
            ),
            (
                "Host disconnection",
                hash(
                    agent,
                    &revision,
                    &inspection.config,
                    false,
                    None,
                    None,
                    &skill_tools,
                    &[],
                ),
            ),
        ];
        for (change, changed_hash) in changed_hashes {
            assert_ne!(changed_hash, snapshot_hash, "{change}");
            let error = paginate_agent_inspection(
                inspection.clone(),
                tools.clone(),
                changed_hash,
                Some(cursor),
                1,
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("inspection snapshot changed"),
                "{change}"
            );
        }
    }
}
