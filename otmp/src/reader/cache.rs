use crate::RuntimeError;
use otmp_protocol::Sha256;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Identity {
    pub uri: String,
    pub hash: Sha256,
    pub length: u64,
    pub version: Option<crate::ObjectVersion>,
}

type Key = (String, u64, u64);

#[derive(Default)]
struct Entries {
    values: BTreeMap<Key, Arc<CachedBytes>>,
    identities: BTreeMap<String, (Identity, usize)>,
    order: VecDeque<Key>,
}

struct Budget {
    used: AtomicUsize,
    peak: AtomicUsize,
    limit: usize,
}

pub(crate) struct Reservation {
    budget: Arc<Budget>,
    amount: usize,
}
impl Reservation {
    #[cfg(test)]
    pub(crate) fn for_test(amount: usize) -> Self {
        Cache::new(usize::MAX).reserve(amount).unwrap()
    }
    pub(crate) fn shrink_to(&mut self, amount: usize) {
        assert!(amount <= self.amount, "a reservation can only shrink");
        self.budget
            .used
            .fetch_sub(self.amount - amount, Ordering::AcqRel);
        self.amount = amount;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.amount, Ordering::AcqRel);
    }
}

pub(crate) struct CachedBytes {
    bytes: Vec<u8>,
    _reservation: Reservation,
}

impl AsRef<[u8]> for CachedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// FIFO admission avoids allocating a new queue entry on every cache hit.
pub(super) struct Cache {
    budget: Arc<Budget>,
    entries: Mutex<Entries>,
}

impl Cache {
    pub fn new(limit: usize) -> Self {
        Self {
            budget: Arc::new(Budget {
                used: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                limit,
            }),
            entries: Mutex::new(Entries::default()),
        }
    }
    pub fn used(&self) -> usize {
        self.budget.used.load(Ordering::Acquire)
    }
    pub fn peak(&self) -> usize {
        self.budget.peak.load(Ordering::Acquire)
    }

    pub fn reserve(&self, amount: usize) -> Result<Reservation, RuntimeError> {
        let mut entries = self.entries.lock().unwrap();
        loop {
            let used = self.used();
            if let Some(next) = used.checked_add(amount).filter(|n| *n <= self.budget.limit) {
                if self
                    .budget
                    .used
                    .compare_exchange(used, next, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.budget.peak.fetch_max(next, Ordering::AcqRel);
                    return Ok(Reservation {
                        budget: self.budget.clone(),
                        amount,
                    });
                }
                continue;
            }
            let Some(key) = entries.order.pop_front() else {
                return Err(RuntimeError::ResourceExhausted(format!(
                    "metadata cache: {amount} requested, {used} active, {} limit",
                    self.budget.limit
                )));
            };
            entries.values.remove(&key);
            if let Some((_, count)) = entries.identities.get_mut(&key.0) {
                *count -= 1;
                if *count == 0 {
                    entries.identities.remove(&key.0);
                }
            }
        }
    }

    fn check(entries: &Entries, identity: &Identity) -> Result<(), RuntimeError> {
        if entries
            .identities
            .get(&identity.uri)
            .is_some_and(|(old, _)| old != identity)
        {
            return Err(RuntimeError::Corrupt(
                "conflicting cached object reference".into(),
            ));
        }
        Ok(())
    }

    pub fn get(
        &self,
        identity: &Identity,
        range: &Range<u64>,
    ) -> Result<Option<Arc<CachedBytes>>, RuntimeError> {
        let entries = self.entries.lock().unwrap();
        Self::check(&entries, identity)?;
        Ok(entries
            .values
            .get(&(identity.uri.clone(), range.start, range.end))
            .cloned())
    }

    pub fn insert(
        &self,
        identity: Identity,
        range: Range<u64>,
        bytes: Vec<u8>,
        reservation: Reservation,
    ) -> Result<Arc<CachedBytes>, RuntimeError> {
        if bytes
            .capacity()
            .checked_add(256 + identity.uri.len() * 4)
            .is_none_or(|size| size > reservation.amount)
        {
            return Err(RuntimeError::ResourceExhausted(
                "cache allocation exceeded reservation".into(),
            ));
        }
        let mut entries = self.entries.lock().unwrap();
        Self::check(&entries, &identity)?;
        let key = (identity.uri.clone(), range.start, range.end);
        if let Some(value) = entries.values.get(&key) {
            return Ok(value.clone());
        }
        let value = Arc::new(CachedBytes {
            bytes,
            _reservation: reservation,
        });
        entries
            .identities
            .entry(identity.uri.clone())
            .or_insert((identity, 0))
            .1 += 1;
        entries.order.push_back(key.clone());
        entries.values.insert(key, value.clone());
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_reservations_survive_eviction_and_release_on_drop() {
        let cache = Cache::new(1024);
        let held = cache.reserve(800).unwrap();
        assert!(cache.reserve(225).is_err());
        assert_eq!(cache.used(), 800);
        drop(held);
        assert_eq!(cache.used(), 0);
        assert!(cache.reserve(1024).is_ok());
    }

    #[test]
    fn reference_conflicts_are_checked_on_cache_hits() {
        let cache = Cache::new(8192);
        let first = Identity {
            uri: "node".into(),
            hash: otmp_protocol::Sha256::digest(b"abcd"),
            length: 4,
            version: None,
        };
        cache
            .insert(
                first.clone(),
                0..4,
                b"abcd".to_vec(),
                cache.reserve(512).unwrap(),
            )
            .unwrap();
        assert!(cache.get(&first, &(0..4)).unwrap().is_some());
        let conflicting = Identity { length: 5, ..first };
        assert!(cache.get(&conflicting, &(0..4)).is_err());
    }

    #[test]
    fn eviction_keeps_held_bytes_accounted_then_allows_a_checked_replay() {
        let cache = Cache::new(1024);
        let first = Identity {
            uri: "node".into(),
            hash: otmp_protocol::Sha256::digest(b"abcd"),
            length: 4,
            version: None,
        };
        let held = cache
            .insert(
                first.clone(),
                0..4,
                b"abcd".to_vec(),
                cache.reserve(512).unwrap(),
            )
            .unwrap();
        // Admission evicts the map entry, but the returned Arc still owns its
        // reservation and therefore prevents over-admission.
        assert!(cache.reserve(600).is_err());
        assert_eq!(cache.used(), 512);
        drop(held);
        assert_eq!(cache.used(), 0);

        cache
            .insert(
                first.clone(),
                0..4,
                b"abcd".to_vec(),
                cache.reserve(512).unwrap(),
            )
            .unwrap();
        assert!(cache.get(&first, &(0..4)).unwrap().is_some());
        let conflicting = Identity { length: 5, ..first };
        assert!(cache.get(&conflicting, &(0..4)).is_err());
    }
}
