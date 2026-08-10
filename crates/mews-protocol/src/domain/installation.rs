use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Installation {
    pub id: InstallationId,
    pub public_key: String,
    pub relay_url: Option<String>,
    pub hub_host_id: HostId,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Host {
    pub id: HostId,
    pub name: String,
    pub public_key: String,
    pub noise_public_key: String,
    pub relay_url: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostStatus {
    pub host: Host,
    pub connected: bool,
}

/// One live Harness descriptor paired with the Host that published it. The
/// catalog is connection state, so offline Hosts intentionally have no rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostHarnessStatus {
    pub host: Host,
    pub descriptor: HarnessDescriptor,
}
