//! A bounded counter that releases what it lent when the lease is dropped.
//!
//! Both admission limits are the same shape: a ceiling, a non-blocking
//! acquire, and release tied to the lifetime of the work. Requests count as one
//! each; request bodies count as bytes and may grow as more of the body
//! arrives.

use crate::concurrency::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::fmt;

/// The request would have pushed the counter past its ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetExceeded;

impl fmt::Display for BudgetExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("admission budget exhausted")
    }
}

impl std::error::Error for BudgetExceeded {}

/// A ceiling shared by every producer, enforced without blocking.
///
/// Cloning shares the same ceiling, so holders never handle the `Arc`.
#[derive(Clone, Debug)]
pub struct Budget(Arc<Ceiling>);

#[derive(Debug)]
struct Ceiling {
    used: AtomicUsize,
    capacity: usize,
}

impl Budget {
    /// Creates a budget that lends out at most `capacity` at a time.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self(Arc::new(Ceiling {
            used: AtomicUsize::new(0),
            capacity,
        }))
    }

    /// Takes `amount` from the budget, or reports that it does not fit.
    pub fn try_acquire(&self, amount: usize) -> Result<Lease, BudgetExceeded> {
        self.take(amount)?;
        Ok(Lease {
            budget: self.clone(),
            held: amount,
        })
    }

    /// Currently lent out. Only meaningful as a diagnostic.
    #[must_use]
    pub fn used(&self) -> usize {
        self.0.used.load(Ordering::Relaxed)
    }

    fn take(&self, amount: usize) -> Result<(), BudgetExceeded> {
        let ceiling = &*self.0;
        let mut used = ceiling.used.load(Ordering::Relaxed);
        loop {
            if amount > ceiling.capacity.saturating_sub(used) {
                return Err(BudgetExceeded);
            }
            match ceiling.used.compare_exchange_weak(
                used,
                used + amount,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => used = observed,
            }
        }
    }
}

/// Holds part of a [`Budget`] until dropped.
#[derive(Debug)]
pub struct Lease {
    budget: Budget,
    held: usize,
}

impl Lease {
    /// Raises the lease to `required`, taking only the difference.
    ///
    /// Used while a request body arrives in pieces, so a body that outgrows its
    /// declared length is charged once rather than per chunk.
    pub fn grow_to(&mut self, required: usize) -> Result<(), BudgetExceeded> {
        if required <= self.held {
            return Ok(());
        }
        self.budget.take(required - self.held)?;
        self.held = required;
        Ok(())
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let previous = self.budget.0.used.fetch_sub(self.held, Ordering::AcqRel);
        debug_assert!(previous >= self.held);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn rejects_overcommit_and_releases_capacity() {
        let budget = Budget::new(10);
        let lease = budget.try_acquire(8).expect("first request should fit");
        assert_eq!(budget.try_acquire(3).unwrap_err(), BudgetExceeded);
        drop(lease);
        budget
            .try_acquire(10)
            .expect("dropped lease should release its capacity");
    }

    #[test]
    fn growing_a_lease_charges_only_the_difference() {
        let budget = Budget::new(10);
        let mut lease = budget.try_acquire(0).expect("empty body should fit");
        lease.grow_to(4).expect("first chunk should fit");
        lease.grow_to(4).expect("same size must not charge twice");
        assert_eq!(lease.grow_to(11), Err(BudgetExceeded));
        lease.grow_to(10).expect("remaining budget should fit");
    }

    #[test]
    fn a_single_permit_is_exclusive() {
        let budget = Budget::new(1);
        let permit = budget.try_acquire(1).expect("first request should fit");
        assert_eq!(budget.try_acquire(1).unwrap_err(), BudgetExceeded);
        drop(permit);
        budget
            .try_acquire(1)
            .expect("capacity should be released after completion");
    }
}

#[cfg(loom)]
mod loom_tests {
    use super::*;
    use crate::concurrency::thread;

    /// Concurrent acquires must never lend out more than the ceiling, and every
    /// released lease must be fully returned.
    #[test]
    fn concurrent_acquires_never_exceed_capacity() {
        loom::model(|| {
            let budget = Budget::new(2);
            let threads = (0..2)
                .map(|_| {
                    let budget = budget.clone();
                    thread::spawn(move || {
                        if let Ok(lease) = budget.try_acquire(2) {
                            assert!(budget.used() <= 2);
                            drop(lease);
                        }
                    })
                })
                .collect::<Vec<_>>();

            for handle in threads {
                handle.join().expect("worker should not panic");
            }
            assert_eq!(budget.used(), 0, "every lease must return what it took");
        });
    }

    /// Growing a lease races the same counter as a fresh acquire.
    #[test]
    fn growing_races_acquiring_without_losing_capacity() {
        loom::model(|| {
            let budget = Budget::new(4);
            let mut lease = budget.try_acquire(1).expect("initial lease should fit");

            let other = {
                let budget = budget.clone();
                thread::spawn(move || drop(budget.try_acquire(2)))
            };
            let _ = lease.grow_to(3);
            other.join().expect("worker should not panic");

            drop(lease);
            assert_eq!(budget.used(), 0, "every lease must return what it took");
        });
    }
}
