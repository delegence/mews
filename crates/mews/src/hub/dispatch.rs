use std::{
    fs,
    path::Path,
    sync::{Arc, atomic::Ordering},
};

use anyhow::{Context, Result, bail};

use crate::host::HostControl;
use mews_protocol::{HubRequest, HubResponse, ProtocolError};

use super::{HubRuntime, handoff::*, hub_home, runs};

pub(super) async fn dispatch(
    runtime: &HubRuntime,
    root: &Path,
    request: HubRequest,
) -> Result<(HubResponse, bool)> {
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
        HubRequest::PollEvents {
            consumer_id,
            limit,
            wait_ms,
        } => {
            let events = runs::poll_events(runtime, root, consumer_id, limit, wait_ms).await?;
            return Ok((HubResponse::Events(events), false));
        }
        HubRequest::StartTurn {
            idempotency_key,
            session_id,
            prompt,
            metadata,
            source,
        } => {
            let run = runs::start_turn(
                runtime,
                root,
                idempotency_key,
                session_id,
                prompt,
                metadata,
                source,
            )
            .await?;
            return Ok((HubResponse::Run(run), false));
        }
        HubRequest::ResolvePermission {
            request_id,
            option_id,
        } => {
            let waiter = runtime
                .control
                .permission_waiters
                .lock()
                .await
                .remove(&request_id)
                .with_context(|| {
                    format!("permission request {request_id:?} is no longer pending")
                })?;
            let _ = waiter.send(option_id);
            return Ok((HubResponse::Ack, false));
        }
        request => request,
    };

    let mut mews = runtime.mews.lock().await;
    let response = match request {
        HubRequest::Status => HubResponse::Status(mews.installation()?),
        HubRequest::ListAgents => HubResponse::Agents(mews.agents()?),
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
            HubResponse::Agent(mews.rename_agent(&slug, &new_slug)?)
        }
        HubRequest::ArchiveAgent { slug } => {
            mews.archive_agent(&slug)?;
            HubResponse::Ack
        }
        HubRequest::SetApiKey { provider, key } => {
            mews.set_api_key(&provider, key).await?;
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
        } => {
            mews.subscribe_session(&consumer_id, &session_id)?;
            HubResponse::Ack
        }
        HubRequest::UnsubscribeSession {
            consumer_id,
            session_id,
        } => {
            mews.unsubscribe_session(&consumer_id, &session_id)?;
            HubResponse::Ack
        }
        HubRequest::AcknowledgeEvents {
            consumer_id,
            checkpoint,
        } => {
            mews.acknowledge_events(&consumer_id, checkpoint)?;
            HubResponse::Ack
        }
        HubRequest::GetRun { id } => HubResponse::Run(mews.run(&id)?),
        HubRequest::CancelRun { id } => {
            if let Some(task) = runtime.control.run_tasks.lock().await.remove(&id) {
                task.abort();
            }
            mews.cancel_run(&id)?;
            runtime.control.event_notify.notify_waiters();
            HubResponse::Ack
        }
        HubRequest::ListSessions => HubResponse::Sessions(mews.sessions()?),
        HubRequest::GetSession { id } => HubResponse::Session(mews.session(&id)?),
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
            let directory = working_directory.unwrap_or(hub_home()?);
            HubResponse::Session(mews.start_session(&slug, &directory).await?)
        }
        HubRequest::StartSessionOn {
            slug,
            host_id,
            working_directory,
        } => {
            let installation = mews.installation()?;
            if host_id == installation.hub_host_id {
                HubResponse::Session(mews.start_session(&slug, &working_directory).await?)
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
        | HubRequest::ResolvePermission { .. } => {
            unreachable!("handled before locking")
        }
        HubRequest::ListHosts => {
            let installation = mews.installation()?;
            let connected = runtime.remote_hosts.lock().await;
            HubResponse::Hosts(
                mews.hosts()?
                    .into_iter()
                    .map(|host| crate::HostStatus {
                        connected: host.id == installation.hub_host_id
                            || connected.contains_key(&host.id),
                        host,
                    })
                    .collect(),
            )
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
            let mut catalog = mews_host::HarnessCatalog::discover(Some(root))?
                .descriptors()
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
            let mut catalog = mews_host::HarnessCatalog::discover(Some(root))?
                .descriptors()
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
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            tokio::task::spawn_local(async move {
                if let Err(error) = crate::enrollment::relay::accept_join_ready(
                    &root,
                    offer,
                    ready_tx,
                    remote_hosts,
                    control,
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
            if let Err(error) = transfer_hub_snapshot(&connected, &snapshot).await {
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
            return Ok((response, true));
        }
        HubRequest::Shutdown => return Ok((HubResponse::Ack, true)),
    };
    Ok((response, false))
}
