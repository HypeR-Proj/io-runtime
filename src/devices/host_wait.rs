// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! One persistent console subscription, reconciled after bounded port polling.

/// Observation identity, never authority to access a channel. The source value
/// is the kernel object KOID; interest excludes idle writes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Key {
    pub source: u64,
    pub readable: bool,
    pub writable: bool,
}

pub(super) trait Waits {
    type Id: Copy + Eq;
    type Error;
    fn add(&mut self, key: Key) -> Result<Self::Id, Self::Error>;
    fn remove(&mut self, id: Self::Id) -> Result<(), Self::Error>;
    fn rearm(&mut self, id: Self::Id) -> Result<(), Self::Error>;
}

struct Registered<I> {
    key: Key,
    id: I,
    consumed: bool,
}

pub(super) struct Subscription<I> {
    registered: Option<Registered<I>>,
}

impl<I: Copy + Eq> Subscription<I> {
    pub fn new() -> Self {
        Self { registered: None }
    }

    /// Called only for an event actually dequeued from this wait set.
    pub fn consume(&mut self, id: I) -> bool {
        if let Some(current) = &mut self.registered
            && current.id == id
        {
            current.consumed = true;
            true
        } else {
            false
        }
    }

    pub fn update<W: Waits<Id = I>>(
        &mut self,
        desired: Option<Key>,
        waits: &mut W,
    ) -> Result<(), W::Error> {
        if let Some(current) = &mut self.registered {
            if Some(current.key) == desired {
                if current.consumed {
                    // Rearm observes current levels after polling drained the
                    // previous event. Unconsumed queued events remain intact.
                    waits.rearm(current.id)?;
                    current.consumed = false;
                }
                return Ok(());
            }
            // Cancellation also removes any queued event for the old source.
            // Retain ownership on failure so no registration becomes invisible.
            waits.remove(current.id)?;
            self.registered = None;
        }
        if let Some(key) = desired {
            let id = waits.add(key)?;
            self.registered = Some(Registered {
                key,
                id,
                consumed: false,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/host_wait.rs"]
mod tests;
