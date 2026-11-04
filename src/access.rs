use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::config::Config;

#[derive(Serialize)]
struct ServerRequest<'a> {
    server: &'a str,
    params: &'a serde_json::Map<String, serde_json::Value>,
}

pub struct AccessPolicies {
    config: Arc<Config>,
}

impl AccessPolicies {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    pub async fn allowed(
        &self,
        ids: Option<&BTreeSet<String>>,
        ct: &CancellationToken,
    ) -> BTreeSet<String> {
        let mut allowed = BTreeSet::new();
        let mut groups: BTreeMap<&str, Vec<ServerRequest<'_>>> = BTreeMap::new();
        for (id, server) in &self.config.servers {
            if ids.is_some_and(|ids| !ids.contains(id)) {
                continue;
            }
            match &server.access {
                None => {
                    allowed.insert(id.clone());
                }
                Some(access) => groups
                    .entry(&access.policy)
                    .or_default()
                    .push(ServerRequest {
                        server: id,
                        params: &access.params,
                    }),
            }
        }
        let answers = join_all(groups.into_iter().map(|(name, servers)| async move {
            let requested: BTreeSet<String> = servers
                .iter()
                .map(|server| server.server.to_owned())
                .collect();
            let request = json!({"version": 1, "kind": "access", "servers": servers});
            let result = self.config.access_policies[name]
                .request(name, &request, ct)
                .await;
            #[derive(Deserialize)]
            struct Answer {
                allowed: BTreeSet<String>,
            }
            let answer = result.and_then(|bytes| {
                serde_json::from_slice::<Answer>(&bytes)
                    .map_err(|_| crate::hooks::HookError::Failure("invalid access response"))
            });
            match answer {
                Ok(answer) => answer.allowed.intersection(&requested).cloned().collect(),
                Err(error) => {
                    eprintln!("[gate] access policy `{name}` failed: {error}");
                    BTreeSet::new()
                }
            }
        }))
        .await;
        for answer in answers {
            allowed.extend(answer);
        }
        if ct.is_cancelled() {
            allowed.clear();
        }
        allowed
    }
}
