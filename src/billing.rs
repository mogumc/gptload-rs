use crate::storage::KeyStore;
use ahash::AHashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

pub struct BillingStore {
    balances: Arc<RwLock<AHashMap<String, Arc<AtomicI64>>>>,
    persist_tx: Sender<PersistUpdate>,
}

enum PersistUpdate {
    Set { key: String, balance: i64 },
}

impl BillingStore {
    pub fn new(store: &KeyStore) -> anyhow::Result<Self> {
        let tree = store.open_billing_tree()?;
        let balances = Arc::new(RwLock::new(AHashMap::new()));

        {
            let mut map = balances
                .write()
                .map_err(|_| anyhow::anyhow!("billing balances lock poisoned"))?;
            for item in tree.iter() {
                let (k, v) = item?;
                let key = String::from_utf8_lossy(&k).to_string();
                if let Some(balance) = decode_balance(&v) {
                    map.insert(key, Arc::new(AtomicI64::new(balance)));
                }
            }
        }

        let (tx, rx) = mpsc::channel::<PersistUpdate>();
        let persist_tree = tree.clone();
        thread::spawn(move || {
            let mut pending: AHashMap<String, i64> = AHashMap::new();
            let mut last_flush = Instant::now();
            loop {
                match rx.recv_timeout(Duration::from_millis(500)) {
                    Ok(msg) => match msg {
                        PersistUpdate::Set { key, balance } => {
                            pending.insert(key, balance);
                        }
                    },
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }

                if pending.len() >= 1024 || last_flush.elapsed() >= Duration::from_secs(1) {
                    flush_pending(&persist_tree, &mut pending);
                    last_flush = Instant::now();
                }
            }

            if !pending.is_empty() {
                flush_pending(&persist_tree, &mut pending);
            }
        });

        Ok(Self {
            balances,
            persist_tx: tx,
        })
    }

    pub fn create_key(&self, key: String, balance: i64) -> anyhow::Result<bool> {
        let mut map = self
            .balances
            .write()
            .map_err(|_| anyhow::anyhow!("billing balances lock poisoned"))?;
        if map.contains_key(&key) {
            return Ok(false);
        }
        map.insert(key.clone(), Arc::new(AtomicI64::new(balance)));
        drop(map);
        let _ = self
            .persist_tx
            .send(PersistUpdate::Set { key, balance });
        Ok(true)
    }

    pub fn get_balance(&self, key: &str) -> Option<i64> {
        let map = self.balances.read().ok()?;
        map.get(key).map(|v| v.load(Ordering::Relaxed))
    }

    pub fn adjust_balance(&self, key: &str, delta: i64) -> Option<i64> {
        let map = self.balances.read().ok()?;
        let balance = map.get(key)?.clone();
        drop(map);
        let mut cur = balance.load(Ordering::Relaxed);
        loop {
            let new_balance = cur.saturating_add(delta);
            match balance.compare_exchange(cur, new_balance, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    let _ = self.persist_tx.send(PersistUpdate::Set {
                        key: key.to_string(),
                        balance: new_balance,
                    });
                    return Some(new_balance);
                }
                Err(v) => cur = v,
            }
        }
    }

    pub fn apply_usage(&self, key: &str, total_tokens: u64) -> Option<i64> {
        let delta = i64::try_from(total_tokens).ok()?;
        if delta == 0 {
            return self.get_balance(key);
        }
        self.adjust_balance(key, -delta)
    }
}

fn decode_balance(bytes: &[u8]) -> Option<i64> {
    if bytes.len() == 8 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(bytes);
        Some(i64::from_le_bytes(arr))
    } else {
        None
    }
}

fn flush_pending(tree: &sled::Tree, pending: &mut AHashMap<String, i64>) {
    if pending.is_empty() {
        return;
    }
    for (key, balance) in pending.drain() {
        let encoded = balance.to_le_bytes();
        let _ = tree.insert(key.as_bytes(), &encoded);
    }
    let _ = tree.flush();
}
