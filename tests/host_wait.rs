// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Default)]
struct FakeWaits {
    next: u64,
    current: Option<(u64, Key, bool)>,
    calls: Vec<&'static str>,
    fail: Option<&'static str>,
}

impl Waits for FakeWaits {
    type Id = u64;
    type Error = &'static str;
    fn add(&mut self, key: Key) -> Result<u64, Self::Error> {
        self.calls.push("add");
        if self.fail == Some("add") {
            return Err("add");
        }
        assert!(self.current.is_none());
        self.next += 1;
        self.current = Some((self.next, key, true));
        Ok(self.next)
    }
    fn remove(&mut self, id: u64) -> Result<(), Self::Error> {
        self.calls.push("remove");
        if self.fail == Some("remove") {
            return Err("remove");
        }
        assert_eq!(self.current.map(|v| v.0), Some(id));
        self.current = None;
        Ok(())
    }
    fn rearm(&mut self, id: u64) -> Result<(), Self::Error> {
        self.calls.push("rearm");
        if self.fail == Some("rearm") {
            return Err("rearm");
        }
        assert_eq!(self.current.map(|v| (v.0, v.2)), Some((id, false)));
        if let Some(current) = &mut self.current {
            current.2 = true;
        }
        Ok(())
    }
}

fn key(source: u64, readable: bool, writable: bool) -> Key {
    Key {
        source,
        readable,
        writable,
    }
}

fn consume(waits: &mut FakeWaits, subscription: &mut Subscription<u64>) {
    if let Some(current) = &mut waits.current {
        assert!(current.2);
        current.2 = false;
        assert!(subscription.consume(current.0));
    } else {
        panic!("missing test subscription");
    }
}

#[test]
fn unrelated_mmio_turns_keep_the_armed_or_queued_subscription() -> Result<(), &'static str> {
    let mut waits = FakeWaits::default();
    let mut watch = Subscription::new();
    let desired = Some(key(0x100_0001, true, false));
    for _ in 0..1024 {
        watch.update(desired, &mut waits)?;
    }
    assert_eq!(waits.calls, ["add"]);
    assert!(!watch.consume(99));
    watch.update(desired, &mut waits)?;
    assert_eq!(waits.calls, ["add"]);
    consume(&mut waits, &mut watch);
    watch.update(desired, &mut waits)?;
    watch.update(desired, &mut waits)?;
    assert_eq!(waits.calls, ["add", "rearm"]);
    Ok(())
}

#[test]
fn backpressure_changes_cancel_old_interest_before_replacement() -> Result<(), &'static str> {
    let mut waits = FakeWaits::default();
    let mut watch = Subscription::new();
    watch.update(Some(key(1, true, false)), &mut waits)?;
    // A source event may already be queued. Changing interest cancels it.
    watch.update(Some(key(1, false, true)), &mut waits)?;
    assert!(!watch.consume(1));
    consume(&mut waits, &mut watch);
    // Poll drained output but retained input: only closure remains interesting.
    watch.update(Some(key(1, false, false)), &mut waits)?;
    assert_eq!(waits.calls, ["add", "remove", "add", "remove", "add"]);
    consume(&mut waits, &mut watch);
    watch.update(None, &mut waits)?;
    watch.update(None, &mut waits)?;
    assert_eq!(waits.calls.last(), Some(&"remove"));
    assert!(waits.current.is_none());
    Ok(())
}

#[test]
fn reset_and_replacement_identities_do_not_reuse_old_events() -> Result<(), &'static str> {
    let mut waits = FakeWaits::default();
    let mut watch = Subscription::new();
    watch.update(Some(key(0x100_0007, true, false)), &mut waits)?;
    watch.update(None, &mut waits)?;
    watch.update(Some(key(0x200_0007, true, false)), &mut waits)?;
    assert!(!watch.consume(1));
    assert_eq!(waits.current.map(|v| v.0), Some(2));
    watch.update(Some(key(0x300_0007, true, false)), &mut waits)?;
    assert!(!watch.consume(2));
    assert_eq!(waits.calls, ["add", "remove", "add", "remove", "add"]);
    Ok(())
}

#[test]
fn remove_failure_retains_ownership_and_add_failure_leaves_no_phantom() -> Result<(), &'static str>
{
    let mut waits = FakeWaits::default();
    let mut watch = Subscription::new();
    watch.update(Some(key(1, true, false)), &mut waits)?;
    waits.fail = Some("remove");
    assert_eq!(
        watch.update(Some(key(2, true, false)), &mut waits),
        Err("remove")
    );
    assert_eq!(waits.current.map(|v| v.0), Some(1));
    waits.fail = Some("add");
    assert_eq!(
        watch.update(Some(key(2, true, false)), &mut waits),
        Err("add")
    );
    assert!(waits.current.is_none());
    assert!(!watch.consume(1));
    waits.fail = None;
    watch.update(Some(key(2, true, false)), &mut waits)?;
    assert_eq!(waits.calls, ["add", "remove", "remove", "add", "add"]);
    Ok(())
}

#[test]
fn rearm_failure_preserves_consumed_state_and_close_skips_rearm() -> Result<(), &'static str> {
    let mut waits = FakeWaits::default();
    let mut watch = Subscription::new();
    let desired = Some(key(1, false, false));
    watch.update(desired, &mut waits)?;
    consume(&mut waits, &mut watch);
    waits.fail = Some("rearm");
    assert_eq!(watch.update(desired, &mut waits), Err("rearm"));
    waits.fail = None;
    watch.update(desired, &mut waits)?;
    assert_eq!(waits.calls, ["add", "rearm", "rearm"]);
    consume(&mut waits, &mut watch);
    watch.update(None, &mut waits)?;
    assert_eq!(waits.calls, ["add", "rearm", "rearm", "remove"]);
    Ok(())
}
