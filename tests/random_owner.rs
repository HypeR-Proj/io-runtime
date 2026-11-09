// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hyper_service::random::decode_reply;
use std::cell::{Cell, RefCell};

#[derive(Default)]
struct Transport {
    sent: RefCell<Vec<Vec<u8>>>,
    reply: RefCell<Option<Vec<u8>>>,
    blocked: Cell<bool>,
    closed: Cell<bool>,
    reads: Cell<usize>,
}
impl ControlTransport for Transport {
    fn send(&self, bytes: &[u8]) -> hyper_os::Result<()> {
        if self.closed.get() {
            return Err(Error::Status(Status::PEER_CLOSED));
        }
        if self.blocked.get() {
            return Err(Error::Status(Status::WOULD_BLOCK));
        }
        self.sent.borrow_mut().push(bytes.to_vec());
        Ok(())
    }
    fn receive(&self, bytes: &mut [u8]) -> hyper_os::Result<usize> {
        self.reads.set(self.reads.get() + 1);
        if self.closed.get() {
            return Err(Error::Status(Status::PEER_CLOSED));
        }
        let reply = self
            .reply
            .borrow_mut()
            .take()
            .ok_or(Error::Status(Status::WOULD_BLOCK))?;
        bytes[..reply.len()].copy_from_slice(&reply);
        Ok(reply.len())
    }
}
fn identity() -> OwnerControl {
    OwnerControl {
        binding: 7,
        epoch: 2,
        features: RANDOM_FEATURE,
        last_transaction: 40,
    }
}
fn begin(owner: &mut Owner, until: u64) -> hyper_os::Result<()> {
    owner.begin(Request::new(3, until)?, 0)
}
fn packet(transport: &Transport, status: u32) -> Vec<u8> {
    let sent = transport.sent.borrow();
    let mut reply = sent[sent.len() - 1].clone();
    reply[12..16].copy_from_slice(&1u32.to_le_bytes());
    reply[40..44].copy_from_slice(&status.to_le_bytes());
    reply[44..48].copy_from_slice(&(if status == 0 { 3u32 } else { 0u32 }).to_le_bytes());
    if status == 0 {
        reply.extend_from_slice(&[0xa1, 0xb2, 0xc3]);
    }
    let length = reply.len() as u32;
    reply[8..12].copy_from_slice(&length.to_le_bytes());
    reply
}
fn complete(step: Step, status: Status) -> hyper_os::Result<()> {
    let Step::Complete(reply) = step else {
        return Err(Error::InvalidResponse);
    };
    if status == Status::OK {
        assert_eq!(decode_reply(reply.as_bytes(), 3)?, [0xa1, 0xb2, 0xc3]);
    } else {
        assert_eq!(
            decode_reply(reply.as_bytes(), 3),
            Err(Error::Status(status))
        );
    }
    Ok(())
}

#[test]
fn one_transaction_and_not_ready_requires_fresh_sequence() -> hyper_os::Result<()> {
    let transport = Transport::default();
    let mut owner = Owner::new(Some(identity()));
    begin(&mut owner, 100)?;
    assert!(!owner.available());
    assert!(matches!(owner.interest(), Interest::Write));
    assert_eq!(begin(&mut owner, 100), Err(Error::Status(Status::BUSY)));
    assert!(matches!(owner.poll(&transport, 1)?, Step::Progress));
    assert!(matches!(owner.interest(), Interest::Read));
    assert!(matches!(owner.poll(&transport, 2)?, Step::Waiting));
    assert_eq!(transport.sent.borrow().len(), 1);
    *transport.reply.borrow_mut() = Some(packet(&transport, 6));
    complete(owner.poll(&transport, 3)?, Status::WOULD_BLOCK)?;
    assert!(owner.available());
    begin(&mut owner, 100)?;
    assert!(matches!(owner.poll(&transport, 4)?, Step::Progress));
    let sent = transport.sent.borrow();
    assert_eq!(&sent[0][32..40], &41u64.to_le_bytes());
    assert_eq!(&sent[1][32..40], &42u64.to_le_bytes());
    assert_eq!(&sent[1][6..8], &15u16.to_le_bytes());
    assert_eq!(&sent[1][40..48], &[3, 0, 0, 0, 0, 0, 0, 0]);
    drop(sent);
    *transport.reply.borrow_mut() = Some(packet(&transport, 0));
    complete(owner.poll(&transport, 5)?, Status::OK)?;
    assert!(!owner.take_fault());
    Ok(())
}

#[test]
fn cancellation_before_send_performs_no_io_and_after_send_must_drain() -> hyper_os::Result<()> {
    let transport = Transport::default();
    let mut owner = Owner::new(Some(identity()));
    begin(&mut owner, 100)?;
    owner.cancel();
    assert!(owner.available());
    assert!(transport.sent.borrow().is_empty());
    begin(&mut owner, 100)?;
    owner.poll(&transport, 1)?;
    owner.cancel();
    assert!(!owner.available());
    assert_eq!(begin(&mut owner, 100), Err(Error::Status(Status::BUSY)));
    assert!(matches!(owner.poll(&transport, 2)?, Step::Waiting));
    *transport.reply.borrow_mut() = Some(packet(&transport, 0));
    assert!(matches!(owner.poll(&transport, 3)?, Step::Progress));
    assert!(owner.available());
    begin(&mut owner, 100)?;
    owner.poll(&transport, 4)?;
    assert_eq!(transport.sent.borrow().len(), 2);
    assert_eq!(&transport.sent.borrow()[1][32..40], &43u64.to_le_bytes());
    Ok(())
}

#[test]
fn caller_expiry_before_send_is_safe_but_sent_expiry_retains_reply_owner() -> hyper_os::Result<()> {
    let transport = Transport::default();
    let mut owner = Owner::new(Some(identity()));
    assert_eq!(begin(&mut owner, 0), Err(Error::Status(Status::TIMED_OUT)));
    begin(&mut owner, 3)?;
    complete(owner.poll(&transport, 3)?, Status::TIMED_OUT)?;
    assert!(owner.available());
    assert!(transport.sent.borrow().is_empty());
    begin(&mut owner, 3)?;
    owner.poll(&transport, 1)?;
    complete(owner.poll(&transport, 3)?, Status::TIMED_OUT)?;
    assert!(!owner.available());
    *transport.reply.borrow_mut() = Some(packet(&transport, 0));
    assert!(matches!(owner.poll(&transport, 4)?, Step::Progress));
    assert!(owner.available());
    assert!(!owner.take_fault());
    Ok(())
}

#[test]
fn health_deadline_is_io_error_and_poison_survives_a_late_reply() -> hyper_os::Result<()> {
    for cancel in [false, true] {
        let transport = Transport::default();
        let mut owner = Owner::new(Some(identity()));
        begin(&mut owner, u64::MAX)?;
        owner.poll(&transport, 1)?;
        if cancel {
            owner.cancel();
        }
        assert_eq!(owner.next_deadline(), HEALTH_NS + 1);
        // Already-readable data at the watchdog deadline must not turn into a
        // new success. The poisoned owner alone may drain this late record.
        *transport.reply.borrow_mut() = Some(packet(&transport, 0));
        let step = owner.poll(&transport, HEALTH_NS + 1)?;
        if cancel {
            assert!(matches!(step, Step::Progress));
        } else {
            complete(step, Status::IO_ERROR)?;
        }
        assert!(owner.take_fault());
        assert!(!owner.take_fault());
        assert_eq!(
            begin(&mut owner, u64::MAX),
            Err(Error::Status(Status::IO_ERROR))
        );
        assert!(matches!(owner.interest(), Interest::Read));
        assert!(matches!(
            owner.poll(&transport, HEALTH_NS + 2)?,
            Step::Progress
        ));
        assert!(matches!(owner.interest(), Interest::None));
        assert_eq!(transport.sent.borrow().len(), 1);
        assert_eq!(
            begin(&mut owner, u64::MAX),
            Err(Error::Status(Status::IO_ERROR))
        );
    }
    Ok(())
}

#[test]
fn stalled_admission_is_bounded_without_a_false_caller_timeout() -> hyper_os::Result<()> {
    let transport = Transport::default();
    transport.blocked.set(true);
    let mut owner = Owner::new(Some(identity()));
    begin(&mut owner, u64::MAX)?;
    assert!(matches!(
        owner.poll(&transport, HEALTH_NS - 1)?,
        Step::Waiting
    ));
    assert_eq!(owner.next_deadline(), HEALTH_NS);
    complete(owner.poll(&transport, HEALTH_NS)?, Status::IO_ERROR)?;
    assert!(owner.take_fault());
    assert!(transport.sent.borrow().is_empty());
    assert!(matches!(owner.interest(), Interest::None));
    Ok(())
}

#[test]
fn every_identity_field_and_noncanonical_reply_poison_the_owner() -> hyper_os::Result<()> {
    for index in [0, 4, 6, 8, 12, 16, 24, 32, 40, 44] {
        let transport = Transport::default();
        let mut owner = Owner::new(Some(identity()));
        begin(&mut owner, 100)?;
        owner.poll(&transport, 1)?;
        let mut bad = packet(&transport, 0);
        bad[index] ^= 0x80;
        *transport.reply.borrow_mut() = Some(bad);
        complete(owner.poll(&transport, 2)?, Status::IO_ERROR)?;
        assert!(owner.take_fault());
    }
    for malformed in 0..3 {
        let transport = Transport::default();
        let mut owner = Owner::new(Some(identity()));
        begin(&mut owner, 100)?;
        owner.poll(&transport, 1)?;
        let mut bad = packet(&transport, if malformed == 0 { 6 } else { 0 });
        if malformed == 0 {
            bad[44] = 1;
        } else if malformed == 1 {
            bad.truncate(47);
        } else {
            bad.push(0);
        }
        *transport.reply.borrow_mut() = Some(bad);
        complete(owner.poll(&transport, 2)?, Status::IO_ERROR)?;
        assert!(owner.take_fault());
    }
    Ok(())
}

#[test]
fn declared_backend_errors_are_not_protocol_faults() -> hyper_os::Result<()> {
    for (wire, native) in [
        (1, Status::NOT_SUPPORTED),
        (2, Status::INVALID_ARGUMENT),
        (3, Status::IO_ERROR),
        (4, Status::IO_ERROR),
        (5, Status::IO_ERROR),
        (6, Status::WOULD_BLOCK),
    ] {
        let transport = Transport::default();
        let mut owner = Owner::new(Some(identity()));
        begin(&mut owner, 100)?;
        owner.poll(&transport, 1)?;
        *transport.reply.borrow_mut() = Some(packet(&transport, wire));
        complete(owner.poll(&transport, 2)?, native)?;
        assert!(owner.available());
        assert!(!owner.take_fault());
    }
    Ok(())
}

#[test]
fn closed_transport_overflow_and_unsupported_never_draw_replacement_bytes() -> hyper_os::Result<()>
{
    for close_after_send in [false, true] {
        let transport = Transport::default();
        let mut owner = Owner::new(Some(identity()));
        begin(&mut owner, 100)?;
        if close_after_send {
            owner.poll(&transport, 1)?;
        }
        transport.closed.set(true);
        complete(owner.poll(&transport, 2)?, Status::IO_ERROR)?;
        assert!(owner.take_fault());
        assert_eq!(begin(&mut owner, 100), Err(Error::Status(Status::IO_ERROR)));
    }
    let mut id = identity();
    id.last_transaction = u64::MAX;
    let mut owner = Owner::new(Some(id));
    assert_eq!(begin(&mut owner, 100), Err(Error::Status(Status::IO_ERROR)));
    assert!(owner.take_fault());
    let mut old = identity();
    old.features = 7;
    for id in [None, Some(old)] {
        let mut owner = Owner::new(id);
        let transport = Transport::default();
        assert_eq!(
            begin(&mut owner, 100),
            Err(Error::Status(Status::NOT_SUPPORTED))
        );
        assert!(matches!(owner.poll(&transport, 1)?, Step::Waiting));
        assert!(transport.sent.borrow().is_empty());
        assert_eq!(transport.reads.get(), 0);
        assert!(!owner.take_fault());
    }
    Ok(())
}
