use ruvoy_poc::{
    BridgeError,
    fiber::{DEFAULT_MAX_INFLIGHT_REQUESTS, FiberRuntime, FiberRuntimeClient},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

const DEFAULT_MAX_INFLIGHT_BODY_BYTES: usize = 256 * 1024 * 1024;

pub(crate) struct FiberRackConfig {
    client: FiberRuntimeClient,
    body_budget: Arc<BodyBudget>,
    runtime_thread_id: String,
    runtime: Mutex<Option<FiberRuntime>>,
}

impl FiberRackConfig {
    pub(crate) fn start(filter_config: &[u8]) -> Result<Self, BridgeError> {
        let rackup = std::str::from_utf8(filter_config)
            .map_err(|_| BridgeError::Startup("rackup path must contain valid UTF-8".to_owned()))?;
        if rackup.is_empty() {
            return Err(BridgeError::Startup(
                "rackup path must not be empty".to_owned(),
            ));
        }
        let max_inflight_requests =
            positive_env_usize("RUVOY_MAX_INFLIGHT_REQUESTS", DEFAULT_MAX_INFLIGHT_REQUESTS)?;
        let max_inflight_body_bytes = positive_env_usize(
            "RUVOY_MAX_INFLIGHT_BODY_BYTES",
            DEFAULT_MAX_INFLIGHT_BODY_BYTES,
        )?;
        let runtime = FiberRuntime::start_rackup_with_limit(rackup, max_inflight_requests)?;
        let client = runtime.client();
        let runtime_thread_id = runtime.info().rust_thread_id.clone();

        Ok(Self {
            client,
            body_budget: Arc::new(BodyBudget::new(max_inflight_body_bytes)),
            runtime_thread_id,
            runtime: Mutex::new(Some(runtime)),
        })
    }

    pub(crate) fn client(&self) -> FiberRuntimeClient {
        self.client.clone()
    }

    pub(crate) fn runtime_thread_id(&self) -> &str {
        &self.runtime_thread_id
    }

    pub(crate) fn body_budget(&self) -> Arc<BodyBudget> {
        Arc::clone(&self.body_budget)
    }
}

fn positive_env_usize(name: &str, default: usize) -> Result<usize, BridgeError> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    let value = value
        .into_string()
        .map_err(|_| BridgeError::Startup(format!("{name} must contain valid UTF-8")))?;
    let value = value
        .parse::<usize>()
        .map_err(|_| BridgeError::Startup(format!("{name} must be a positive integer")))?;
    if value == 0 {
        return Err(BridgeError::Startup(format!(
            "{name} must be a positive integer"
        )));
    }
    Ok(value)
}

pub(crate) struct BodyBudget {
    reserved: AtomicUsize,
    maximum: usize,
}

impl BodyBudget {
    fn new(maximum: usize) -> Self {
        Self {
            reserved: AtomicUsize::new(0),
            maximum,
        }
    }

    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<BodyLease, BodyBudgetExceeded> {
        self.try_add(bytes)?;
        Ok(BodyLease {
            budget: Arc::clone(self),
            reserved: bytes,
        })
    }

    fn try_add(&self, bytes: usize) -> Result<(), BodyBudgetExceeded> {
        let mut reserved = self.reserved.load(Ordering::Relaxed);
        loop {
            if bytes > self.maximum.saturating_sub(reserved) {
                return Err(BodyBudgetExceeded);
            }
            match self.reserved.compare_exchange_weak(
                reserved,
                reserved + bytes,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => reserved = observed,
            }
        }
    }
}

pub(crate) struct BodyLease {
    budget: Arc<BodyBudget>,
    reserved: usize,
}

impl BodyLease {
    pub(crate) fn ensure_reserved(&mut self, required: usize) -> Result<(), BodyBudgetExceeded> {
        if required <= self.reserved {
            return Ok(());
        }
        let additional = required - self.reserved;
        self.budget.try_add(additional)?;
        self.reserved = required;
        Ok(())
    }
}

impl Drop for BodyLease {
    fn drop(&mut self) {
        let previous = self
            .budget
            .reserved
            .fetch_sub(self.reserved, Ordering::AcqRel);
        debug_assert!(previous >= self.reserved);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BodyBudgetExceeded;

impl Drop for FiberRackConfig {
    fn drop(&mut self) {
        let runtime_slot = match self.runtime.get_mut() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };

        if let Some(runtime) = runtime_slot.take() {
            match runtime.shutdown() {
                Ok(()) => eprintln!("[ruvoy] Fiber runtime stopped"),
                Err(error) => eprintln!("[ruvoy] Fiber runtime shutdown failed: {error}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_budget_rejects_overcommit_and_releases_capacity() {
        let budget = Arc::new(BodyBudget::new(10));
        let lease = budget.try_reserve(8).expect("first body should fit");
        assert!(budget.try_reserve(3).is_err());
        drop(lease);
        budget
            .try_reserve(10)
            .expect("dropped body should release its capacity");
    }

    #[test]
    fn chunked_body_lease_grows_without_double_reserving() {
        let budget = Arc::new(BodyBudget::new(10));
        let mut lease = budget.try_reserve(0).expect("empty body should fit");
        lease.ensure_reserved(4).expect("first chunk should fit");
        lease
            .ensure_reserved(4)
            .expect("same size must not reserve twice");
        assert!(lease.ensure_reserved(11).is_err());
        lease
            .ensure_reserved(10)
            .expect("remaining budget should fit");
    }
}
