#![allow(dead_code)]

use std::path::Path;

pub struct KeyStore {
    db: sled::Db,
}

pub struct AddKeysResult {
    pub inserted: usize,
    pub existed: usize,
    /// Keys that were newly inserted (not previously present).
    pub inserted_keys: Vec<String>,
}

impl KeyStore {
    pub fn open(data_dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let db_path = data_dir.join("keys_db");
        let db = sled::open(db_path)?;
        Ok(Self { db })
    }

    fn tree_name(upstream_id: &str) -> String {
        format!("u:{}", upstream_id)
    }

    pub fn open_upstream_tree(&self, upstream_id: &str) -> anyhow::Result<sled::Tree> {
        let name = Self::tree_name(upstream_id);
        Ok(self.db.open_tree(name)?)
    }

    pub fn open_billing_tree(&self) -> anyhow::Result<sled::Tree> {
        Ok(self.db.open_tree("billing")?)
    }

    pub fn open_key_levels_tree(&self) -> anyhow::Result<sled::Tree> {
        Ok(self.db.open_tree("key_levels")?)
    }

    pub fn open_key_usage_tree(&self) -> anyhow::Result<sled::Tree> {
        Ok(self.db.open_tree("key_usage")?)
    }

    pub fn open_global_stats_tree(&self) -> anyhow::Result<sled::Tree> {
        Ok(self.db.open_tree("global_stats")?)
    }

    /// Get the permission level for a key. Default 0 if not set.
    pub fn get_key_level(&self, key: &str) -> i32 {
        let tree = match self.open_key_levels_tree() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("key_levels tree open failed, defaulting level to 0: {e}");
                return 0;
            }
        };
        tree.get(key.as_bytes())
            .ok()
            .flatten()
            .and_then(|v| {
                if v.len() == 4 {
                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(&v);
                    Some(i32::from_le_bytes(arr))
                } else {
                    tracing::warn!("key_levels corrupt entry for key '{}', defaulting to 0", key);
                    None
                }
            })
            .unwrap_or(0)
    }

    /// Set the permission level for a key.
    /// Valid values: >= 0, or exactly -1 (unlimited).
    pub fn set_key_level(&self, key: &str, level: i32) -> anyhow::Result<()> {
        if level < 0 && level != -1 {
            anyhow::bail!("key level must be >= 0 or -1, got {}", level);
        }
        let tree = self.open_key_levels_tree()?;
        tree.insert(key.as_bytes(), &level.to_le_bytes())?;
        tree.flush()?;
        Ok(())
    }

    /// Monthly usage per key: (total_tokens, credits_micro) as 2×8 LE bytes.
    pub fn get_key_usage(&self, key: &str) -> (u64, i64) {
        let tree = match self.open_key_usage_tree() {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(error = %e, "get_key_usage: failed to open tree");
                return (0, 0);
            }
        };
        match tree.get(key.as_bytes()) {
            Ok(Some(v)) if v.len() == 16 => {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&v[0..8]);  let tokens = u64::from_le_bytes(arr);
                arr.copy_from_slice(&v[8..16]); let credits = i64::from_le_bytes(arr);
                (tokens, credits)
            }
            _ => (0, 0),
        }
    }

    pub fn add_key_usage(&self, key: &str, tokens: u64, credits: i64) -> anyhow::Result<()> {
        let tree = self.open_key_usage_tree()?;
        let (ct, cc) = self.get_key_usage(key);
        let new = [
            (ct + tokens).to_le_bytes(),
            (cc + credits).to_le_bytes(),
        ].concat();
        tree.insert(key.as_bytes(), new.as_slice())?;
        tree.flush()?;
        Ok(())
    }

    /// Check and reset monthly usage if month changed. Also clears global token stats.
    pub fn check_monthly_reset(&self) -> anyhow::Result<()> {
        let tree = self.open_key_usage_tree()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Calendar month: year*12 + month (UTC), using civil_from_days algorithm.
        let days = now / 86_400;
        let z = days + 719468;
        let era = z / 146097;
        let doe = z - era * 146097;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let year = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        let current_month: u64 = year * 12 + month as u64;
        let reset_key = b"__reset_month";
        let last_month = tree.get(reset_key)?.and_then(|v| {
            if v.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&v);
                Some(u64::from_le_bytes(arr))
            } else { None }
        }).unwrap_or(0);

        if last_month != current_month {
            tree.clear()?;
            tree.insert(reset_key, &current_month.to_le_bytes())?;
            tree.flush()?;
            // Also clear global token stats to keep both counters in sync.
            if let Ok(gs) = self.open_global_stats_tree() {
                let _ = gs.clear();
                let _ = gs.flush();
            }
            tracing::info!(last_month, current_month, "monthly key usage reset");
        }
        Ok(())
    }

    /// Load global token counters from persistent storage. Returns (prompt, completion, thought, total).
    pub fn load_global_tokens(&self) -> (u64, u64, u64, u64) {
        let tree = match self.open_global_stats_tree() {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(error = %e, "load_global_tokens: failed to open tree");
                return (0, 0, 0, 0);
            }
        };
        match tree.get(b"tokens") {
            Ok(Some(v)) if v.len() == 32 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v[0..8]);   let a = u64::from_le_bytes(buf);
                buf.copy_from_slice(&v[8..16]);  let b = u64::from_le_bytes(buf);
                buf.copy_from_slice(&v[16..24]); let c = u64::from_le_bytes(buf);
                buf.copy_from_slice(&v[24..32]); let d = u64::from_le_bytes(buf);
                (a, b, c, d)
            }
            _ => (0, 0, 0, 0),
        }
    }

    /// Save global token counters to persistent storage.
    pub fn save_global_tokens(&self, prompt: u64, completion: u64, thought: u64, total: u64) -> anyhow::Result<()> {
        let tree = self.open_global_stats_tree()?;
        let mut buf = Vec::with_capacity(32);
        buf.extend_from_slice(&prompt.to_le_bytes());
        buf.extend_from_slice(&completion.to_le_bytes());
        buf.extend_from_slice(&thought.to_le_bytes());
        buf.extend_from_slice(&total.to_le_bytes());
        tree.insert(b"tokens", buf.as_slice())?;
        tree.flush()?;
        Ok(())
    }

    pub fn count_keys(&self, upstream_id: &str) -> anyhow::Result<usize> {
        let t = self.open_upstream_tree(upstream_id)?;
        Ok(t.len())
    }

    /// Add keys. Keys are unique by DB key; duplicates are counted as `existed`.
    ///
    /// Returns (inserted, existed, inserted_keys).
    pub fn add_keys(&self, upstream_id: &str, keys: &[String]) -> anyhow::Result<AddKeysResult> {
        let t = self.open_upstream_tree(upstream_id)?;
        let mut inserted = 0usize;
        let mut existed = 0usize;
        let mut inserted_keys = Vec::new();

        for k in keys {
            let kb = k.as_bytes();
            let prev = t.insert(kb, &[] as &[u8])?;
            if prev.is_none() {
                inserted += 1;
                inserted_keys.push(k.clone());
            } else {
                existed += 1;
            }
        }
        t.flush()?;
        Ok(AddKeysResult {
            inserted,
            existed,
            inserted_keys,
        })
    }

    /// Replace all keys for upstream with the provided list.
    pub fn replace_keys(&self, upstream_id: &str, keys: &[String]) -> anyhow::Result<()> {
        let t = self.open_upstream_tree(upstream_id)?;
        t.clear()?;
        for k in keys {
            t.insert(k.as_bytes(), &[] as &[u8])?;
        }
        t.flush()?;
        Ok(())
    }

    pub fn delete_keys(&self, upstream_id: &str, keys: &[String]) -> anyhow::Result<usize> {
        let t = self.open_upstream_tree(upstream_id)?;
        let mut removed = 0usize;
        for k in keys {
            if t.remove(k.as_bytes())?.is_some() {
                removed += 1;
            }
        }
        t.flush()?;
        Ok(removed)
    }

    pub fn load_all_keys(&self, upstream_id: &str) -> anyhow::Result<Vec<String>> {
        let t = self.open_upstream_tree(upstream_id)?;
        let mut out = Vec::with_capacity(t.len());
        for item in t.iter() {
            let (k, _v) = item?;
            let s = std::str::from_utf8(&k).map_err(|_| {
                anyhow::anyhow!("invalid utf-8 key in db for upstream {}", upstream_id)
            })?;
            out.push(s.to_string());
        }
        Ok(out)
    }

    /// Export DB to a JSON file (best-effort). Useful for backup.
    pub fn export_json(&self, path: &Path) -> anyhow::Result<()> {
        use serde::Serialize;
        use std::collections::BTreeMap;

        #[derive(Serialize)]
        struct Export {
            upstreams: BTreeMap<String, Vec<String>>,
        }

        let mut upstreams: BTreeMap<String, Vec<String>> = BTreeMap::new();

        for name in self.db.tree_names() {
            let name = String::from_utf8_lossy(&name).to_string();
            if !name.starts_with("u:") {
                continue;
            }
            let upstream_id = name.trim_start_matches("u:").to_string();
            let t = self.db.open_tree(&name)?;
            let mut keys = Vec::with_capacity(t.len());
            for item in t.iter() {
                let (k, _v) = item?;
                keys.push(String::from_utf8_lossy(&k).to_string());
            }
            upstreams.insert(upstream_id, keys);
        }

        let export = Export { upstreams };
        let s = serde_json::to_string_pretty(&export)?;
        std::fs::write(path, s)?;
        Ok(())
    }

    /// Import keys from a JSON file. This replaces keys for upstreams included in the file.
    pub fn import_json(&self, path: &Path) -> anyhow::Result<()> {
        use serde::Deserialize;
        use std::collections::BTreeMap;

        #[derive(Deserialize)]
        struct Export {
            upstreams: BTreeMap<String, Vec<String>>,
        }

        let s = std::fs::read_to_string(path)?;
        let export: Export = serde_json::from_str(&s)?;

        for (upstream_id, keys) in export.upstreams {
            self.replace_keys(&upstream_id, &keys)?;
        }
        Ok(())
    }

    pub fn flush(&self) -> anyhow::Result<()> {
        self.db.flush()?;
        Ok(())
    }
}
