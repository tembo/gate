//! Steps that run before gate connects to a server, such as joining the network it's on

use std::{
    fmt,
    sync::{Arc, Mutex},
};

use futures::future::{BoxFuture, FutureExt, Shared};
use tokio_util::sync::CancellationToken;

use crate::hooks::Hook;

type Run = Shared<BoxFuture<'static, Result<(), Arc<str>>>>;

pub struct PreConnect {
    name: String,
    hook: Arc<Hook>,
    running: Mutex<Option<Run>>,
}

impl fmt::Debug for PreConnect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreConnect")
            .field("name", &self.name)
            .field("hook", &self.hook)
            .finish_non_exhaustive()
    }
}

impl PreConnect {
    pub fn new(name: String, hook: Hook) -> Self {
        Self {
            name,
            hook: Arc::new(hook),
            running: Mutex::new(None),
        }
    }

    /// Runs the step before a connection attempt. Overlapping attempts share one run, and
    /// later attempts run it again, so the step can restore what it set up.
    pub async fn run(&self) -> Result<(), Arc<str>> {
        let run = {
            let mut running = self.running.lock().unwrap();
            running
                .get_or_insert_with(|| {
                    let (name, hook) = (self.name.clone(), self.hook.clone());
                    async move {
                        hook.run(&CancellationToken::new()).await.map_err(|error| {
                            Arc::<str>::from(format!("pre-connect step `{name}` failed: {error}"))
                        })
                    }
                    .boxed()
                    .shared()
                })
                .clone()
        };
        let result = run.clone().await;
        let mut running = self.running.lock().unwrap();
        if running.as_ref().is_some_and(|current| current.ptr_eq(&run)) {
            *running = None;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::*;
    use crate::hooks::RawHook;

    fn step(script: &str, log: &std::path::Path) -> PreConnect {
        let hook = serde_json::from_value::<RawHook>(
            json!({"command": "/bin/sh", "args": ["-c", script, "net", log]}),
        )
        .unwrap()
        .resolve_command(&|_| None, &mut BTreeSet::new(), 5)
        .unwrap();
        PreConnect::new("net".into(), hook)
    }

    #[tokio::test]
    async fn overlapping_attempts_share_a_run_and_later_ones_run_again() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("runs");
        let runs = || std::fs::read_to_string(&log).unwrap().lines().count();
        let step = step("echo run >> \"$1\"; sleep 0.3", &log);

        let (first, second) = tokio::join!(step.run(), step.run());
        first.unwrap();
        second.unwrap();
        assert_eq!(runs(), 1);
        step.run().await.unwrap();
        assert_eq!(runs(), 2);
    }

    #[tokio::test]
    async fn failures_name_the_step() {
        let dir = tempfile::tempdir().unwrap();
        let error = step("exit 1", &dir.path().join("runs"))
            .run()
            .await
            .unwrap_err();
        assert_eq!(&*error, "pre-connect step `net` failed: command failed");
    }
}
