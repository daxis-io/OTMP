//! Bounded models of OTMP-owned transitions, not Tokio/Shared internals.
//! Registry: footer.rs `FooterLoad::drop/pending_load` and reader/pages.rs `RangeLoad`.
//! Budget: footer.rs `Reservation::{mark_key,shrink,finish_transient,drop}`.
//! Admission: footer.rs admit enables notification before checking pressure and
//! holds the FIFO turn across waits. Mutex/Condvar below stand in for Tokio's
//! documented FIFO turn and Notify; they do not attempt to verify those primitives.
use loom::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use loom::thread;
use std::collections::VecDeque;

struct Registry {
    slot: Mutex<Option<usize>>,
    // Loom has no Weak. A zero strong count cannot be upgraded; identities
    // remain in the registry until remove-if-same runs, just like std::Weak.
    strong: [AtomicUsize; 2],
}
impl Registry {
    fn release(&self, id: usize) {
        if self.strong[id].fetch_sub(1, Ordering::AcqRel) == 1 {
            thread::yield_now(); // Last strong owner dies before destructor cleanup.
            let mut slot = self.slot.lock().unwrap();
            if *slot == Some(id) {
                *slot = None;
            }
        }
    }
    fn join_or_replace(&self) -> usize {
        let mut slot = self.slot.lock().unwrap();
        if let Some(id) = *slot {
            let mut count = self.strong[id].load(Ordering::Acquire);
            while count != 0 {
                match self.strong[id].compare_exchange(
                    count,
                    count + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return id,
                    Err(actual) => count = actual,
                }
            }
        }
        self.strong[1].store(1, Ordering::Release);
        *slot = Some(1);
        1
    }
}
#[test]
fn old_load_cleanup_cannot_remove_a_replacement_after_weak_upgrade_fails() {
    loom::model(|| {
        let registry = Arc::new(Registry {
            slot: Mutex::new(Some(0)),
            strong: [AtomicUsize::new(1), AtomicUsize::new(0)],
        });
        let old = registry.clone();
        let dropping = thread::spawn(move || old.release(0));
        let joining = registry.clone();
        let replacing = thread::spawn(move || joining.join_or_replace());
        dropping.join().unwrap();
        let id = replacing.join().unwrap();
        assert_eq!(*registry.slot.lock().unwrap(), Some(id));
        assert_eq!(registry.strong[id].load(Ordering::Acquire), 1);
        registry.release(id);
        assert_eq!(*registry.slot.lock().unwrap(), None);
    });
}

#[derive(Clone, Copy)]
enum Phase {
    Payload,
    Key,
    Preflight,
    Retained,
}
struct Budget {
    used: AtomicUsize,
    payloads: AtomicUsize,
    keys: AtomicUsize,
    hits: AtomicUsize,
}
struct Reservation {
    budget: Arc<Budget>,
    amount: usize,
    phase: Phase,
}
impl Budget {
    fn hit(budget: &Arc<Self>) -> Reservation {
        budget.hits.fetch_add(1, Ordering::AcqRel);
        Reservation {
            budget: budget.clone(),
            amount: 0,
            phase: Phase::Preflight,
        }
    }
    fn reserve(budget: &Arc<Self>, amount: usize) -> Option<Reservation> {
        let mut used = budget.used.load(Ordering::Acquire);
        loop {
            let next = used.checked_add(amount).filter(|n| *n <= 8)?;
            match budget
                .used
                .compare_exchange(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    budget.payloads.fetch_add(1, Ordering::AcqRel);
                    return Some(Reservation {
                        budget: budget.clone(),
                        amount,
                        phase: Phase::Payload,
                    });
                }
                Err(actual) => used = actual,
            }
        }
    }
}
impl Reservation {
    fn finish(&mut self) {
        match std::mem::replace(&mut self.phase, Phase::Retained) {
            Phase::Payload => {
                assert!(self.budget.payloads.fetch_sub(1, Ordering::AcqRel) > 0);
            }
            Phase::Key => {
                assert!(self.budget.keys.fetch_sub(1, Ordering::AcqRel) > 0);
            }
            Phase::Preflight => {
                assert!(self.budget.hits.fetch_sub(1, Ordering::AcqRel) > 0);
            }
            Phase::Retained => {}
        }
    }
    fn key(&mut self) {
        self.budget.keys.fetch_add(1, Ordering::AcqRel);
        self.finish();
        self.phase = Phase::Key;
    }
    fn shrink(&mut self, amount: usize) {
        self.amount -= amount;
        assert!(self.budget.used.fetch_sub(amount, Ordering::AcqRel) >= amount);
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        assert!(self.budget.used.fetch_sub(self.amount, Ordering::AcqRel) >= self.amount);
        self.finish();
    }
}
fn budget() -> Arc<Budget> {
    Arc::new(Budget {
        used: AtomicUsize::new(0),
        payloads: AtomicUsize::new(0),
        keys: AtomicUsize::new(0),
        hits: AtomicUsize::new(0),
    })
}
fn empty(b: &Budget) {
    assert_eq!(b.used.load(Ordering::Acquire), 0);
    assert_eq!(b.payloads.load(Ordering::Acquire), 0);
    assert_eq!(b.keys.load(Ordering::Acquire), 0);
    assert_eq!(b.hits.load(Ordering::Acquire), 0);
}
#[test]
fn reserve_shrink_retain_and_drop_preserve_phase_accounting() {
    // Bound preemptions rather than truncating permutations. Two competing
    // allocations and the final key drop cover the named accounting races.
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.check(|| {
        let b = budget();
        let mut key = Budget::reserve(&b, 1).unwrap();
        key.key();
        let worker = b.clone();
        let first = thread::spawn(move || {
            let _hit = Budget::hit(&worker);
            if let Some(mut payload) = Budget::reserve(&worker, 6) {
                payload.shrink(4);
                payload.finish();
                let retained = Arc::new(payload);
                let active_lease = retained.clone();
                drop(retained); // Cache eviction cannot release the active lease.
                assert!(worker.used.load(Ordering::Acquire) >= 2);
                drop(active_lease);
            }
        });
        let worker = b.clone();
        let second = thread::spawn(move || {
            if let Some(payload) = Budget::reserve(&worker, 5) {
                assert!(worker.used.load(Ordering::Acquire) <= 8);
                drop(payload); // Cancellation before retention.
            }
        });
        drop(key);
        first.join().unwrap();
        second.join().unwrap();
        empty(&b);
    });
}
#[test]
fn cancelling_one_shared_waiter_preserves_key_and_payload_until_last_owner() {
    loom::model(|| {
        let b = budget();
        let mut key = Budget::reserve(&b, 1).unwrap();
        key.key();
        let load = Arc::new((key, Mutex::new(Budget::reserve(&b, 6).unwrap())));
        let cancelled = load.clone();
        let cancellation = thread::spawn(move || drop(cancelled));
        let completing = load.clone();
        let completion = thread::spawn(move || {
            let mut payload = completing.1.lock().unwrap();
            payload.shrink(4);
            payload.finish();
        });
        drop(load);
        cancellation.join().unwrap();
        completion.join().unwrap();
        empty(&b);
    });
}

struct Admission {
    queue: VecDeque<usize>,
    used: usize,
    notified: bool,
    waiting: bool,
}
#[test]
fn fifo_pressure_wakeup_and_cancelled_head_do_not_strand_the_next_waiter() {
    loom::model(|| {
        let state = Arc::new((
            Mutex::new(Admission {
                queue: VecDeque::from([0, 1]),
                used: 8,
                notified: false,
                waiting: false,
            }),
            Condvar::new(),
        ));
        let waiting = state.clone();
        let waiter = thread::spawn(move || {
            let (lock, wake) = &*waiting;
            let mut state = lock.lock().unwrap();
            loop {
                // enable() precedes pressure inspection. The FIFO turn stays
                // at queue.front while waiting; later smaller requests cannot pass.
                state.waiting = true;
                if state.queue.front() == Some(&1) && state.used + 2 <= 8 {
                    state.used += 2;
                    state.queue.pop_front();
                    state.waiting = false;
                    break;
                }
                if state.notified {
                    state.notified = false;
                } else {
                    state = wake.wait(state).unwrap();
                }
            }
            assert!(state.used <= 8);
            state.used -= 2;
        });
        let releasing = state.clone();
        let release = thread::spawn(move || {
            let (lock, wake) = &*releasing;
            let mut state = lock.lock().unwrap();
            state.used -= 2; // Reservation::shrink followed by notify_waiters.
            state.notified = state.waiting;
            wake.notify_all();
        });
        let cancelling = state.clone();
        let cancel = thread::spawn(move || {
            let (lock, wake) = &*cancelling;
            let mut state = lock.lock().unwrap();
            assert_eq!(state.queue.pop_front(), Some(0));
            state.notified = state.waiting;
            wake.notify_all();
        });
        release.join().unwrap();
        cancel.join().unwrap();
        waiter.join().unwrap();
        let mut final_state = state.0.lock().unwrap();
        assert!(final_state.queue.is_empty());
        assert!(!final_state.waiting);
        assert_eq!(final_state.used, 6);
        final_state.used -= 6; // Final retained lease drop.
        assert_eq!(final_state.used, 0);
    });
}
