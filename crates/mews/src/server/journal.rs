use std::{path::Path, time::Duration};

use anyhow::Result;

use crate::app::Mews;

use super::HubRuntime;

pub(super) async fn poll_journal_entries(
    runtime: &HubRuntime,
    root: &Path,
    mut query: mews_protocol::JournalQuery,
    wait_ms: u32,
) -> Result<mews_protocol::JournalPage> {
    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(u64::from(wait_ms.min(30_000)));
    loop {
        let notified = runtime.control.journal_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // A journal watch must not fence Hub handoff while it waits. The read
        // itself still participates in the same movement gate as other calls.
        let operation_guard = runtime.control.handoff_gate.read().await;
        if runtime
            .control
            .moving
            .load(std::sync::atomic::Ordering::Acquire)
        {
            anyhow::bail!("Hub is moving; try again after handoff");
        }
        let page = Mews::open_connection(root)?.journal_entries_page(&query)?;
        drop(operation_guard);

        if !page.entries.is_empty() || tokio::time::Instant::now() >= deadline {
            return Ok(page);
        }
        // Even a filtered-out journal event advances the exclusive cursor.
        query.after = page.cursor;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let _ = tokio::time::timeout(remaining, notified).await;
    }
}
