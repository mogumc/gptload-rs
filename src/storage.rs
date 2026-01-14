
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
            let s = std::str::from_utf8(&k)
                .map_err(|_| anyhow::anyhow!("invalid utf-8 key in db for upstream {}", upstream_id))?;
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
