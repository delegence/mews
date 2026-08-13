use std::{io::Write, path::Path};

use anyhow::Result;
use mews_client::MewsClient;

use super::command::{JournalCommand, JournalQueryArgs};

pub async fn run(root: &Path, command: JournalCommand) -> Result<()> {
    let mut client = MewsClient::connect(root).await?;
    match command {
        JournalCommand::List { query } => {
            let page = client.query_journal_entries(journal_query(query)).await?;
            println!("{}", serde_json::to_string_pretty(&page)?);
        }
        JournalCommand::Watch { query } => {
            let mut query = journal_query(query);
            loop {
                let page = client.poll_journal_entries(query.clone(), 30_000).await?;
                query.after = page.cursor;
                if !page.entries.is_empty() {
                    println!("{}", serde_json::to_string(&page)?);
                    std::io::stdout().flush()?;
                }
            }
        }
    }
    Ok(())
}

fn journal_query(query: JournalQueryArgs) -> mews_protocol::JournalQuery {
    mews_protocol::JournalQuery {
        after: query.after,
        limit: query.limit,
        filter: mews_protocol::JournalFilter {
            subject_type: query.subject_type,
            subject_id: query.subject_id,
            event_types: query.event_types,
            session_id: query.session,
            correlation_id: query.correlation,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_filters_map_without_losing_the_resume_cursor() {
        let query = journal_query(JournalQueryArgs {
            after: 17,
            limit: 8,
            subject_type: Some(mews_protocol::JournalSubjectType::Agent),
            subject_id: Some("agt_test".into()),
            event_types: vec![mews_protocol::JournalEventType::AgentRenamed],
            session: None,
            correlation: Some("command-1".into()),
        });

        assert_eq!(query.after, 17);
        assert_eq!(query.limit, 8);
        assert_eq!(
            query.filter.subject_type,
            Some(mews_protocol::JournalSubjectType::Agent)
        );
        assert_eq!(query.filter.subject_id.as_deref(), Some("agt_test"));
        assert_eq!(
            query.filter.event_types,
            [mews_protocol::JournalEventType::AgentRenamed]
        );
        assert_eq!(query.filter.correlation_id.as_deref(), Some("command-1"));
    }
}
