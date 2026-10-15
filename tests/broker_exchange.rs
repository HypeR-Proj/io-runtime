// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::protocol::control::Command;
use std::cell::RefCell;

#[derive(Default)]
struct Transport {
    sent: RefCell<Vec<Vec<u8>>>,
    reply: RefCell<Option<Vec<u8>>>,
    blocked: bool,
}
impl ControlTransport for Transport {
    fn send(&self, bytes: &[u8]) -> hyper_os::Result<()> {
        if self.blocked {
            return Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK));
        }
        self.sent.borrow_mut().push(bytes.to_vec());
        Ok(())
    }
    fn receive(&self, bytes: &mut [u8]) -> hyper_os::Result<usize> {
        let reply = self
            .reply
            .borrow_mut()
            .take()
            .ok_or(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK))?;
        bytes[..reply.len()].copy_from_slice(&reply);
        Ok(reply.len())
    }
}
fn pending() -> Pending {
    Pending {
        request: Request {
            binding: 1,
            epoch: 1,
            transaction: 7,
            command: Command::Prepare {
                alias: 0,
                guest_base: 0x4000_0000,
                length: 0x800_0000,
                mapping_token: 8,
            },
        },
        original: None,
        sent: false,
        limit: 100,
    }
}
fn reply(request: Request) -> Vec<u8> {
    let mut bytes = [0; MAX_RECORD];
    request.encode(&mut bytes).unwrap();
    bytes[8..12].copy_from_slice(&48u32.to_le_bytes());
    bytes[12..16].copy_from_slice(&1u32.to_le_bytes());
    bytes[40..48].fill(0);
    bytes[..48].to_vec()
}
#[test]
fn delayed_client_does_not_block_second_transaction() {
    let (a, b) = (Transport::default(), Transport::default());
    let (mut first, mut second) = (pending(), pending());
    assert!(matches!(first.poll(&a, 1).unwrap(), Step::Sent));
    assert!(matches!(second.poll(&b, 1).unwrap(), Step::Sent));
    *b.reply.borrow_mut() = Some(reply(second.request));
    assert!(matches!(first.poll(&a, 2).unwrap(), Step::Waiting));
    assert!(matches!(second.poll(&b, 2).unwrap(), Step::Reply(_)));
    assert_eq!(a.sent.borrow().len(), 1);
}
#[test]
fn sent_prepare_cannot_be_cancelled_and_expiry_does_not_reuse_lane() {
    let transport = Transport::default();
    let mut request = pending();
    assert!(request.can_cancel());
    assert!(matches!(request.poll(&transport, 1).unwrap(), Step::Sent));
    assert!(!request.can_cancel());
    for now in 2..100 {
        assert!(matches!(
            request.poll(&transport, now).unwrap(),
            Step::Waiting
        ));
    }
    assert!(request.poll(&transport, 100).is_err());
    assert!(!request.can_cancel());
    assert_eq!(transport.sent.borrow().len(), 1);
}
#[test]
fn ready_completion_wins_deadline_but_wrong_identity_never_does() {
    let transport = Transport::default();
    let mut request = pending();
    request.poll(&transport, 1).unwrap();
    *transport.reply.borrow_mut() = Some(reply(request.request));
    assert!(matches!(
        request.poll(&transport, 100).unwrap(),
        Step::Reply(_)
    ));
    let mut bad = reply(request.request);
    bad[32..40].copy_from_slice(&99u64.to_le_bytes());
    *transport.reply.borrow_mut() = Some(bad);
    assert!(request.poll(&transport, 101).is_err());
}
#[test]
fn unwritable_mailbox_has_bounded_deadline_and_remains_cancellable() {
    let transport = Transport {
        blocked: true,
        ..Transport::default()
    };
    let mut request = pending();
    assert!(matches!(
        request.poll(&transport, 1).unwrap(),
        Step::Waiting
    ));
    assert!(request.can_cancel());
    assert!(request.poll(&transport, 100).is_err());
    assert!(transport.sent.borrow().is_empty());
}
