use anyhow::{Result, bail};

use super::Mews;

const SCAN_BATCH: usize = 256;

impl Mews {
    /// Reads the audit journal after an exclusive journal cursor. Filtering
    /// happens while scanning so the returned cursor can safely advance past
    /// unrelated events without skipping a future match.
    pub fn journal_entries_page(
        &self,
        query: &mews_protocol::JournalQuery,
    ) -> Result<mews_protocol::JournalPage> {
        let limit = usize::from(query.limit.clamp(1, 500));
        let mut cursor = query.after;
        let mut payload_bytes = 0;
        let mut entries = Vec::new();

        loop {
            let batch = self.store.journal_entries_after(cursor, SCAN_BATCH)?;
            if batch.is_empty() {
                return Ok(mews_protocol::JournalPage {
                    entries,
                    cursor,
                    has_more: false,
                });
            }
            let batch_is_full = batch.len() == SCAN_BATCH;
            for entry in batch {
                if matches_filter(&entry, &query.filter) {
                    // Once the page is full, scan until the next match so
                    // `has_more` describes the filtered result set. Unrelated
                    // entries still advance the resume cursor safely.
                    if entries.len() == limit {
                        return Ok(mews_protocol::JournalPage {
                            entries,
                            cursor,
                            has_more: true,
                        });
                    }
                    let entry_bytes = serde_json::to_vec(&entry)?.len();
                    if entry_bytes > mews_protocol::MAX_JOURNAL_PAGE_PAYLOAD_BYTES {
                        bail!("Journal entry exceeds the Hub page limit");
                    }
                    let separator = usize::from(!entries.is_empty());
                    if payload_bytes + separator + entry_bytes
                        > mews_protocol::MAX_JOURNAL_PAGE_PAYLOAD_BYTES
                    {
                        return Ok(mews_protocol::JournalPage {
                            entries,
                            cursor,
                            has_more: true,
                        });
                    }
                    payload_bytes += separator + entry_bytes;
                    cursor = entry.position;
                    entries.push(entry);
                } else {
                    cursor = entry.position;
                }
            }
            if !batch_is_full {
                return Ok(mews_protocol::JournalPage {
                    entries,
                    cursor,
                    has_more: false,
                });
            }
        }
    }
}

fn matches_filter(
    entry: &mews_protocol::JournalEntry,
    filter: &mews_protocol::JournalFilter,
) -> bool {
    filter
        .subject_type
        .is_none_or(|subject_type| entry.subject.kind == subject_type)
        && filter
            .subject_id
            .as_deref()
            .is_none_or(|subject_id| entry.subject.id == subject_id)
        && (filter.event_types.is_empty() || filter.event_types.contains(&entry.event_type))
        && filter.session_id.as_ref().is_none_or(|session_id| {
            entry.subject.kind == mews_protocol::JournalSubjectType::Session
                && entry.subject.id == session_id.as_str()
        })
        && filter
            .correlation_id
            .as_deref()
            .is_none_or(|correlation_id| entry.correlation_id.as_deref() == Some(correlation_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filtered_pages_advance_an_exclusive_journal_cursor() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        mews.create_agent("first").unwrap();
        mews.create_agent("second").unwrap();
        mews.create_agent("third").unwrap();

        let filter = mews_protocol::JournalFilter {
            event_types: vec![mews_protocol::JournalEventType::AgentCreated],
            ..Default::default()
        };
        let first = mews
            .journal_entries_page(&mews_protocol::JournalQuery {
                after: 0,
                limit: 2,
                filter: filter.clone(),
            })
            .unwrap();
        assert_eq!(first.entries.len(), 2);
        assert!(first.has_more);
        assert!(first.entries.iter().all(|entry| {
            entry.event_type == mews_protocol::JournalEventType::AgentCreated
                && entry.position <= first.cursor
        }));

        let second = mews
            .journal_entries_page(&mews_protocol::JournalQuery {
                after: first.cursor,
                limit: 2,
                filter,
            })
            .unwrap();
        assert_eq!(second.entries.len(), 1);
        assert!(second.entries[0].position > first.cursor);
        assert!(!second.has_more);
    }

    #[test]
    fn empty_filtered_page_advances_past_unrelated_entries() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        mews.create_agent("first").unwrap();
        let query = mews_protocol::JournalQuery {
            after: 0,
            limit: 10,
            filter: mews_protocol::JournalFilter {
                correlation_id: Some("not-present".into()),
                ..Default::default()
            },
        };

        let page = mews.journal_entries_page(&query).unwrap();

        assert!(page.entries.is_empty());
        assert!(page.cursor > query.after);
        assert!(!page.has_more);
    }

    #[test]
    fn full_filtered_page_only_reports_more_for_another_match() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        mews.create_agent("only").unwrap();
        mews.rename_agent("only", "renamed").unwrap();
        let page = mews
            .journal_entries_page(&mews_protocol::JournalQuery {
                after: 0,
                limit: 1,
                filter: mews_protocol::JournalFilter {
                    event_types: vec![mews_protocol::JournalEventType::AgentCreated],
                    ..Default::default()
                },
            })
            .unwrap();

        assert_eq!(page.entries.len(), 1);
        assert!(!page.has_more);
        assert!(page.cursor > page.entries[0].position);
    }
}
