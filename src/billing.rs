use crate::storage::KeyStore;
use ahash::AHashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

/// Balance unit: 1 credit = 1,000,000 micro-credits (6 decimal places).
pub const MICRO_PER_CREDIT: i64 = 1_000_000;
/// Maximum balance credits that fit in i64 when converted to micro-credits.
pub const MAX_BALANCE_CREDITS: i64 = i64::MAX / MICRO_PER_CREDIT;
/// Minimum charge: 1 micro-credit.
const MIN_COST_MICRO: i64 = 1;

pub struct BillingStore {
    balances: Arc<RwLock<AHashMap<String, Arc<AtomicI64>>>>,
    persist_tx: Sender<PersistUpdate>,
}

pub enum ReserveResult {
    Reserved,
    Insufficient,
    Missing,
}

enum PersistUpdate {
    Set { key: String, balance: i64 },
    Delete { key: String },
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
                        PersistUpdate::Delete { key } => {
                            pending.remove(&key);
                            let _ = persist_tree.remove(key.as_bytes());
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
        let _ = self.persist_tx.send(PersistUpdate::Set { key, balance });
        Ok(true)
    }

    pub fn delete_key(&self, key: &str) -> anyhow::Result<bool> {
        let mut map = self
            .balances
            .write()
            .map_err(|_| anyhow::anyhow!("billing balances lock poisoned"))?;
        if map.remove(key).is_some() {
            drop(map);
            let _ = self.persist_tx.send(PersistUpdate::Delete {
                key: key.to_string(),
            });
            Ok(true)
        } else {
            Ok(false)
        }
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
        // -1 means unlimited, reject any adjustment to protect the sentinel
        if cur == -1 {
            return Some(-1);
        }
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

    pub fn reserve_request(&self, key: &str) -> ReserveResult {
        let map = match self.balances.read() {
            Ok(map) => map,
            Err(_) => return ReserveResult::Missing,
        };
        let Some(balance) = map.get(key).cloned() else {
            return ReserveResult::Missing;
        };
        drop(map);

        let mut cur = balance.load(Ordering::Relaxed);
        // -1 means unlimited, always succeed without decrementing
        if cur == -1 {
            return ReserveResult::Reserved;
        }
        loop {
            if cur < MICRO_PER_CREDIT {
                return ReserveResult::Insufficient;
            }
            let new_balance = cur.saturating_sub(MICRO_PER_CREDIT);
            match balance.compare_exchange(cur, new_balance, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    tracing::debug!(
                        key, cur, new_balance,
                        "billing: reserve key={key} {cur} → {new_balance}"
                    );
                    let _ = self.persist_tx.send(PersistUpdate::Set {
                        key: key.to_string(),
                        balance: new_balance,
                    });
                    return ReserveResult::Reserved;
                }
                Err(v) => cur = v,
            }
        }
    }

    pub fn list_keys(&self) -> Vec<(String, i64)> {
        let map = match self.balances.read() {
            Ok(m) => m,
            Err(_) => return Vec::new(),
        };
        let mut keys: Vec<(String, i64)> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect();
        keys.sort_by(|a, b| a.0.cmp(&b.0));
        keys
    }

    pub fn release_reservation(&self, key: &str) -> Option<i64> {
        // -1 means unlimited, nothing to release
        if self.get_balance(key) == Some(-1) {
            return Some(-1);
        }
        let result = self.adjust_balance(key, MICRO_PER_CREDIT);
        tracing::debug!(key, ?result, "billing: release key={key}");
        result
    }

    /// Settle reserved usage with model-aware credit calculation.
    /// Cost = ceil((prompt_tokens × input_rate + completion_tokens × output_rate) / 1000).
    /// Minimum cost is 1 credit.
    pub fn settle_reserved_usage(
        &self,
        key: &str,
        prompt_tokens: u64,
        completion_tokens: u64,
        model: &str,
        model_costs: &ahash::AHashMap<String, crate::config::ModelCost>,
    ) -> Option<i64> {
        let map = self.balances.read().ok()?;
        let balance = map.get(key)?.clone();
        drop(map);

        let cost = compute_credit_cost(prompt_tokens, completion_tokens, model, model_costs);

        let mut cur = balance.load(Ordering::Relaxed);
        // -1 means unlimited, no adjustment
        if cur == -1 {
            return Some(-1);
        }
        // Cost is positive (micro-credits); reserve already deducted MICRO_PER_CREDIT.
        // Net adjustment = MICRO_PER_CREDIT - cost
        let adjustment = MICRO_PER_CREDIT.saturating_sub(cost);
        loop {
            let new_balance = cur.saturating_add(adjustment);
            let clamped = if new_balance < 0 { 0 } else { new_balance };
            match balance.compare_exchange(cur, clamped, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    tracing::debug!(
                        key, cur, new_balance, clamped, cost, adjustment,
                        "billing: settle key={} cost={} cur={} adj={} → {clamped}",
                        key, cost, cur, adjustment
                    );
                    let _ = self.persist_tx.send(PersistUpdate::Set {
                        key: key.to_string(),
                        balance: clamped,
                    });
                    return Some(clamped);
                }
                Err(v) => cur = v,
            }
        }
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

/// Cost in micro-credits: ceil((prompt×input + completion×output)/1000 × 1_000_000).
/// Unknown models fall back to input=0, output=1.0. Minimum 1 micro-credit.
pub fn compute_credit_cost(
    prompt_tokens: u64,
    completion_tokens: u64,
    model: &str,
    model_costs: &ahash::AHashMap<String, crate::config::ModelCost>,
) -> i64 {
    let rate = model_costs.get(model);
    let input_rate = rate.map(|r| r.input).unwrap_or(0.0);
    let output_rate = rate.map(|r| r.output).unwrap_or(1.0);

    let raw = (prompt_tokens as f64 * input_rate + completion_tokens as f64 * output_rate) / 1000.0;
    let cost = (raw * MICRO_PER_CREDIT as f64).ceil() as i64;
    let final_cost = cost.max(MIN_COST_MICRO);
    tracing::debug!(
        model, prompt_tokens, completion_tokens, input_rate, output_rate,
        raw, cost, final_cost,
        "billing: micro_credits={final_cost}"
    );
    final_cost
}
