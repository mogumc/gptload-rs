use super::*;

pub(super) fn load_upstreams_override(path: &Path) -> anyhow::Result<Vec<UpstreamConfig>> {
    let s = std::fs::read_to_string(path)?;
    let list: Vec<UpstreamConfig> = serde_json::from_str(&s)?;
    Ok(list)
}

pub(super) fn write_upstreams_override(path: &Path, upstreams: &[UpstreamConfig]) -> anyhow::Result<()> {
    let s = serde_json::to_string_pretty(upstreams)?;
    std::fs::write(path, s)?;
    Ok(())
}

pub(super) fn write_model_routes(path: &Path, routes: &ModelRoutesFile) -> anyhow::Result<()> {
    let s = serde_json::to_string_pretty(routes)?;
    std::fs::write(path, s)?;
    Ok(())
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct ModelRoutesFile {
    pub updated_at_ms: u64,
    pub models: BTreeMap<String, Vec<String>>,
    pub upstreams: BTreeMap<String, Vec<String>>,
}

pub(super) fn load_model_routes(path: &Path) -> anyhow::Result<ModelRoutesFile> {
    let s = std::fs::read_to_string(path)?;
    let routes: ModelRoutesFile = serde_json::from_str(&s)?;
    Ok(routes)
}

/// Build the reverse index: upstream_id→models  →  model→[upstream_ids] (sorted, deduped).
pub(super) fn build_models_index(
    upstreams: &BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, Vec<String>> {
    let mut models: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (id, list) in upstreams {
        for model in list {
            models.entry(model.clone()).or_default().push(id.clone());
        }
    }
    for ids in models.values_mut() {
        ids.sort();
        ids.dedup();
    }
    models
}

pub(super) fn apply_loaded_routes(
    routes: &ModelRoutesFile,
    upstreams: &[Arc<Upstream>],
    upstream_index: &AHashMap<String, usize>,
) {
    apply_routes_to_upstreams(routes, upstreams, upstream_index);
}

pub(super) fn routes_has_upstream(routes: &ModelRoutesFile, upstream_id: &str) -> bool {
    routes.upstreams.contains_key(upstream_id)
}

impl RouterState {
    pub async fn refresh_missing_models_routes(&self) {
        let routes = load_model_routes(&self.model_routes_path).ok();
        let mut refreshed = 0usize;
        let snap = self.snapshot.load_full();
        for u in snap.upstreams.iter() {
            if routes
                .as_ref()
                .map(|r| routes_has_upstream(r, u.id.as_ref()))
                .unwrap_or(false)
            {
                continue;
            }
            if let Err(e) = self.refresh_models_for_upstream(u.clone()).await {
                tracing::warn!(upstream = %u.id, error = %e, "model refresh failed");
            } else {
                refreshed += 1;
            }
        }
        if refreshed > 0 {
            if let Err(e) = self.persist_model_routes() {
                tracing::warn!(error = %e, "model routes persist failed");
            }
        }
    }

    pub async fn refresh_missing_models_for_upstream(&self, upstream_id: &str) {
        let routes = load_model_routes(&self.model_routes_path).ok();
        if routes
            .as_ref()
            .map(|r| routes_has_upstream(r, upstream_id))
            .unwrap_or(false)
        {
            return;
        }
        if let Err(e) = self.refresh_models_by_id(upstream_id).await {
            tracing::warn!(upstream = %upstream_id, error = %e, "model refresh failed");
        }
    }

    pub async fn refresh_models_by_id(&self, upstream_id: &str) -> anyhow::Result<usize> {
        let Some((_idx, upstream)) = self.upstream_by_id(upstream_id) else {
            anyhow::bail!("unknown upstream id");
        };
        let count = self.refresh_models_for_upstream(upstream).await?;
        if let Err(e) = self.persist_model_routes() {
            tracing::warn!(error = %e, "model routes persist failed");
        }
        Ok(count)
    }

    pub async fn fetch_models_preview(&self, upstream_id: &str) -> anyhow::Result<Vec<String>> {
        let Some((_idx, upstream)) = self.upstream_by_id(upstream_id) else {
            anyhow::bail!("unknown upstream id");
        };
        let models = self.fetch_models_for_upstream(upstream).await?;
        let mut list: Vec<String> = models.into_iter().collect();
        list.sort();
        Ok(list)
    }

    async fn refresh_models_for_upstream(&self, upstream: Arc<Upstream>) -> anyhow::Result<usize> {
        let models = self.fetch_models_for_upstream(upstream.clone()).await?;
        let count = models.len();
        upstream.models.store(Arc::new(models));
        Ok(count)
    }
}

impl RouterState {
    pub fn get_model_routes(&self) -> ModelRoutesFile {
        match load_model_routes(&self.model_routes_path) {
            Ok(routes) => routes,
            Err(_) => self.build_model_routes(),
        }
    }

    pub fn save_model_routes(
        &self,
        upstreams: BTreeMap<String, Vec<String>>,
    ) -> anyhow::Result<ModelRoutesFile> {
        let snap = self.snapshot.load_full();
        for id in upstreams.keys() {
            if !snap.upstream_index.contains_key(id) {
                anyhow::bail!("unknown upstream id: {}", id);
            }
        }

        let mut upstreams_clean: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (id, models) in upstreams {
            let mut set = AHashSet::with_capacity(models.len().max(1));
            for m in models {
                let m = m.trim();
                if !m.is_empty() {
                    set.insert(m.to_string());
                }
            }
            let mut list: Vec<String> = set.into_iter().collect();
            list.sort();
            upstreams_clean.insert(id, list);
        }

        let models = build_models_index(&upstreams_clean);

        let routes = ModelRoutesFile {
            updated_at_ms: now_ms(),
            models,
            upstreams: upstreams_clean,
        };

        write_model_routes(&self.model_routes_path, &routes)?;
        apply_routes_to_upstreams(&routes, &snap.upstreams, &snap.upstream_index);
        Ok(routes)
    }

    pub fn add_upstream(&self, cfg: UpstreamConfig) -> anyhow::Result<()> {
        let _guard = self
            .admin_write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("admin write lock poisoned"))?;
        let mut list = self.current_upstream_configs();
        if list.iter().any(|u| u.id == cfg.id) {
            anyhow::bail!("upstream id already exists");
        }
        list.push(cfg);
        self.replace_upstreams(list)?;
        Ok(())
    }

    pub fn update_upstream(&self, id: &str, mut cfg: UpstreamConfig) -> anyhow::Result<()> {
        let _guard = self
            .admin_write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("admin write lock poisoned"))?;
        let mut list = self.current_upstream_configs();
        let mut found = false;
        cfg.id = id.to_string();
        if cfg.format.is_none() {
            cfg.format = Some(UpstreamFormat::detect(&cfg.base_url));
        }
        for u in list.iter_mut() {
            if u.id == id {
                *u = cfg.clone();
                found = true;
                break;
            }
        }
        if !found {
            anyhow::bail!("unknown upstream id");
        }
        self.replace_upstreams(list)?;
        Ok(())
    }

    pub fn delete_upstream(&self, id: &str, delete_keys: bool) -> anyhow::Result<()> {
        let _guard = self
            .admin_write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("admin write lock poisoned"))?;
        let mut list = self.current_upstream_configs();
        let before = list.len();
        list.retain(|u| u.id != id);
        if list.len() == before {
            anyhow::bail!("unknown upstream id");
        }
        self.replace_upstreams(list)?;
        if delete_keys {
            let empty: Vec<String> = Vec::new();
            self.store.replace_keys(id, &empty)?;
        }
        Ok(())
    }

    pub async fn test_key_by_value(
        &self,
        upstream_id: &str,
        key_value: &str,
    ) -> anyhow::Result<bool> {
        let Some((_idx, upstream)) = self.upstream_by_id(upstream_id) else {
            anyhow::bail!("unknown upstream id");
        };
        let key_value = key_value.trim();
        if key_value.is_empty() {
            anyhow::bail!("key must not be empty");
        }
        let key = build_key_states(vec![key_value.to_string()])?
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("invalid key"))?;
        validate_key(
            &upstream,
            &key,
            Duration::from_secs(self.key_config().revalidation_timeout_secs.max(5)),
        )
        .await
    }

    fn build_model_routes(&self) -> ModelRoutesFile {
        let mut upstreams: BTreeMap<String, Vec<String>> = BTreeMap::new();

        let snap = self.snapshot.load_full();
        for u in snap.upstreams.iter() {
            let set = u.models.load_full();
            if set.is_empty() {
                continue;
            }
            let mut model_list: Vec<String> = set.iter().cloned().collect();
            model_list.sort();
            model_list.dedup();
            upstreams.insert(u.id.to_string(), model_list);
        }

        let models = build_models_index(&upstreams);

        ModelRoutesFile {
            updated_at_ms: now_ms(),
            models,
            upstreams,
        }
    }

    fn persist_model_routes(&self) -> anyhow::Result<()> {
        if !self.any_models_loaded() {
            return Ok(());
        }
        let routes = self.build_model_routes();
        write_model_routes(&self.model_routes_path, &routes)?;
        Ok(())
    }

    fn replace_upstreams(&self, configs: Vec<UpstreamConfig>) -> anyhow::Result<()> {
        let snapshot = build_snapshot_from_configs(&configs, &self.store)?;
        if let Ok(routes) = load_model_routes(&self.model_routes_path) {
            apply_routes_to_upstreams(&routes, &snapshot.upstreams, &snapshot.upstream_index);
        }
        self.snapshot.store(Arc::new(snapshot));
        write_upstreams_override(&self.upstreams_path, &configs)?;
        self.cleanup_model_routes()?;
        Ok(())
    }

    fn current_upstream_configs(&self) -> Vec<UpstreamConfig> {
        let snap = self.snapshot.load_full();
        snap.upstreams
            .iter()
            .map(|u| UpstreamConfig {
                id: u.id.to_string(),
                base_url: u.base_url.to_string(),
                weight: Some(u.weight),
                max_concurrent_per_key: if u.max_concurrent_per_key > 0 {
                    Some(u.max_concurrent_per_key)
                } else {
                    None
                },
                format: Some(u.format),
                proxy: u.proxy.clone(),
                model_map: u.model_map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                min_key_level: u.min_key_level,
                custom_headers: u.custom_headers.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            })
            .collect()
    }

    fn cleanup_model_routes(&self) -> anyhow::Result<()> {
        let Ok(mut routes) = load_model_routes(&self.model_routes_path) else {
            return Ok(());
        };
        let snap = self.snapshot.load_full();
        let mut changed = false;
        routes.upstreams.retain(|id, _| {
            let keep = snap.upstream_index.contains_key(id);
            if !keep {
                changed = true;
            }
            keep
        });
        if !changed {
            return Ok(());
        }
        routes.models = build_models_index(&routes.upstreams);
        routes.updated_at_ms = now_ms();
        write_model_routes(&self.model_routes_path, &routes)?;
        apply_routes_to_upstreams(&routes, &snap.upstreams, &snap.upstream_index);
        Ok(())
    }

    async fn fetch_models_for_upstream(
        &self,
        upstream: Arc<Upstream>,
    ) -> anyhow::Result<AHashSet<String>> {
        let keys = upstream.active_keys.load_full();
        let key = keys
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no active keys for upstream"))?;

        let pq: http::uri::PathAndQuery = match upstream.format {
            UpstreamFormat::Openai | UpstreamFormat::Anthropic => {
                http::uri::PathAndQuery::from_static("/v1/models")
            }
            UpstreamFormat::Gemini => {
                let path = format!("/v1beta/models?key={}", url_encode(key.key.as_ref()));
                path.parse()?
            }
        };
        let uri = upstream.build_uri(&pq)?;
        let mut req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Body::empty())?;
        insert_key_headers(req.headers_mut(), upstream.format, &key)?;

        let resp = match tokio::time::timeout(self.request_timeout(), upstream.client.request(req))
            .await
        {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => anyhow::bail!("upstream request timeout"),
        };
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("upstream returned {}", status);
        }

        let body = hyper::body::to_bytes(resp.into_body()).await?;
        parse_models_response(upstream.format, &body)
    }
}

pub(super) fn build_snapshot_from_configs(
    configs: &[UpstreamConfig],
    store: &KeyStore,
) -> anyhow::Result<RouterSnapshot> {
    const MAX_WEIGHT: usize = 100;

    let mut upstreams: Vec<Arc<Upstream>> = Vec::new();
    let mut upstream_index: AHashMap<String, usize> = AHashMap::new();
    let mut schedule: Vec<usize> = Vec::new();

    for u_cfg in configs.iter().cloned() {
        if upstream_index.contains_key(&u_cfg.id) {
            anyhow::bail!("duplicate upstream id: {}", u_cfg.id);
        }
        let weight = u_cfg.weight.unwrap_or(1).clamp(1, MAX_WEIGHT);
        let u = parse_upstream(u_cfg, weight)?;
        let idx = upstreams.len();
        upstream_index.insert(u.id.to_string(), idx);

        let keys = store.load_all_keys(&u.id)?;
        let key_states = build_key_states(keys)?;
        // active_keys starts as a copy of all keys (all active by default).
        let active = key_states.iter().cloned().collect::<Vec<_>>();
        u.keys.store(key_states);
        u.active_keys.store(Arc::new(active));

        for _ in 0..weight {
            schedule.push(idx);
        }
        upstreams.push(u);
    }

    Ok(RouterSnapshot {
        upstreams,
        upstream_index,
        schedule,
    })
}

pub(super) fn apply_routes_to_upstreams(
    routes: &ModelRoutesFile,
    upstreams: &[Arc<Upstream>],
    upstream_index: &AHashMap<String, usize>,
) {
    let mut per_upstream: Vec<AHashSet<String>> = vec![AHashSet::new(); upstreams.len()];
    for (id, models) in &routes.upstreams {
        if let Some(idx) = upstream_index.get(id) {
            for model in models {
                let m = model.trim();
                if !m.is_empty() {
                    per_upstream[*idx].insert(m.to_string());
                }
            }
        }
    }
    for (idx, set) in per_upstream.into_iter().enumerate() {
        upstreams[idx].models.store(Arc::new(set));
    }
}

impl RouterState {
    /// Update model costs at runtime (via admin API).
    pub fn set_model_costs(&self, costs: ahash::AHashMap<String, crate::config::ModelCost>) {
        let old = self.runtime.load_full();
        let mut rt: RuntimeConfig = (*old).clone();

        // Persist to disk before storing (clone for serialization).
        let ser_costs: std::collections::HashMap<&str, &crate::config::ModelCost> =
            costs.iter().map(|(k, v)| (k.as_str(), v)).collect();
        if let Ok(json) = serde_json::to_string_pretty(&ser_costs) {
            if let Err(e) = std::fs::write(&self.model_costs_path, &json) {
                tracing::warn!(path = %self.model_costs_path.display(), error = %e, "failed to persist model costs");
            }
        }

        rt.model_costs = costs;
        self.runtime.store(Arc::new(rt));
    }
}
