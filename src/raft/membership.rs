use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::{fs, io};

use openraft::BasicNode;

/// Persisted cluster membership for auto-rejoin on restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMembership {
    pub node_id: u64,
    pub advertise_addr: String,
    pub peers: BTreeMap<u64, String>,
}

const CLUSTER_FILE: &str = "cluster.json";

impl ClusterMembership {
    /// Load membership from data directory, if it exists.
    pub fn load(data_dir: &Path) -> Option<Self> {
        let path = data_dir.join(CLUSTER_FILE);
        let data = fs::read_to_string(&path).ok()?;
        serde_json::from_str(&data).ok()
    }

    /// Persist membership to data directory.
    pub fn save(&self, data_dir: &Path) -> io::Result<()> {
        let path = data_dir.join(CLUSTER_FILE);
        let data = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(&path, data)?;
        Ok(())
    }

    /// Delete persisted membership (on leave).
    pub fn delete(data_dir: &Path) -> io::Result<()> {
        let path = data_dir.join(CLUSTER_FILE);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Convert peers to openraft's BTreeMap<NodeId, BasicNode>.
    pub fn to_members(&self) -> BTreeMap<u64, BasicNode> {
        let mut members = BTreeMap::new();
        members.insert(self.node_id, BasicNode {
            addr: self.advertise_addr.clone(),
        });
        for (&id, addr) in &self.peers {
            members.insert(id, BasicNode {
                addr: addr.clone(),
            });
        }
        members
    }
}

/// Info about a single node in the cluster.
#[derive(Debug, Clone, Serialize)]
pub struct NodeInfo {
    pub node_id: u64,
    pub addr: String,
    pub role: String, // "leader", "follower", "learner"
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_save_and_load() {
        let dir = TempDir::new().unwrap();
        let membership = ClusterMembership {
            node_id: 1,
            advertise_addr: "127.0.0.1:5433".to_string(),
            peers: {
                let mut m = BTreeMap::new();
                m.insert(2, "127.0.0.1:5434".to_string());
                m.insert(3, "127.0.0.1:5435".to_string());
                m
            },
        };

        membership.save(dir.path()).unwrap();
        let loaded = ClusterMembership::load(dir.path()).unwrap();
        assert_eq!(loaded.node_id, 1);
        assert_eq!(loaded.peers.len(), 2);
    }

    #[test]
    fn test_load_missing() {
        let dir = TempDir::new().unwrap();
        assert!(ClusterMembership::load(dir.path()).is_none());
    }

    #[test]
    fn test_delete() {
        let dir = TempDir::new().unwrap();
        let membership = ClusterMembership {
            node_id: 1,
            advertise_addr: "127.0.0.1:5433".to_string(),
            peers: BTreeMap::new(),
        };
        membership.save(dir.path()).unwrap();
        assert!(ClusterMembership::load(dir.path()).is_some());

        ClusterMembership::delete(dir.path()).unwrap();
        assert!(ClusterMembership::load(dir.path()).is_none());
    }

    #[test]
    fn test_to_members() {
        let membership = ClusterMembership {
            node_id: 1,
            advertise_addr: "127.0.0.1:5433".to_string(),
            peers: {
                let mut m = BTreeMap::new();
                m.insert(2, "127.0.0.1:5434".to_string());
                m
            },
        };
        let members = membership.to_members();
        assert_eq!(members.len(), 2);
        assert!(members.contains_key(&1));
        assert!(members.contains_key(&2));
    }
}
