// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Serialized owner transactions. Client cancellation never reuses an unread reply.

use crate::{broker_exchange::ControlTransport, handoff::OwnerControl, protocol::control};
use hyper_os::{Error, Status};
use hyper_service::random::{MAX_BYTES, Reply, Request};

const HEALTH_NS: u64 = 5_000_000_000;
const RANDOM_FEATURE: u64 = 1 << 3;

#[derive(Clone, Copy)]
struct Transaction {
    wire: [u8; 48],
    bytes: usize,
    deadline: u64,
    health: u64,
}

enum Phase {
    Idle,
    Sending(Transaction),
    Awaiting(Transaction),
    Draining(Transaction),
    // A timed-out sent request may still produce one late response. Never
    // restart this sequence after uncertain completion or attribute it anew.
    Poisoned { drain: bool },
    Unsupported,
}

// A reply is bounded to 224 bytes; returning it inline avoids a heap allocation
// for every entropy request. No large unbounded payload belongs in this enum.
#[allow(clippy::large_enum_variant)]
pub enum Step {
    Waiting,
    Progress,
    Complete(Reply),
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Interest {
    Read,
    Write,
    None,
}

pub struct Owner {
    identity: Option<OwnerControl>,
    phase: Phase,
    fault_reported: bool,
}

struct Record([u8; control::MAX_RECORD]);
impl Drop for Record {
    fn drop(&mut self) {
        for byte in &mut self.0 {
            // SAFETY: exclusively owned initialized bytes; volatile prevents
            // removal of this best-effort erasure of our local payload copy.
            unsafe { core::ptr::write_volatile(byte, 0) };
        }
    }
}
impl Owner {
    pub fn new(identity: Option<OwnerControl>) -> Self {
        let supported = identity
            .as_ref()
            .is_some_and(|id| id.features & RANDOM_FEATURE != 0);
        Self {
            identity,
            phase: if supported {
                Phase::Idle
            } else {
                Phase::Unsupported
            },
            fault_reported: false,
        }
    }
    pub fn available(&self) -> bool {
        matches!(
            self.phase,
            Phase::Idle | Phase::Unsupported | Phase::Poisoned { .. }
        )
    }
    pub fn begin(&mut self, request: Request, now: u64) -> hyper_os::Result<()> {
        if now >= request.deadline() {
            return Err(Error::Status(Status::TIMED_OUT));
        }
        match self.phase {
            Phase::Unsupported => return Err(Error::Status(Status::NOT_SUPPORTED)),
            Phase::Poisoned { .. } => return Err(Error::Status(Status::IO_ERROR)),
            Phase::Idle => (),
            _ => return Err(Error::Status(Status::BUSY)),
        }
        let Some(identity) = &mut self.identity else {
            return Err(Error::InvalidResponse);
        };
        let Some(transaction) = identity.last_transaction.checked_add(1) else {
            self.phase = Phase::Poisoned { drain: false };
            return Err(Error::Status(Status::IO_ERROR));
        };
        let mut record = [0; control::MAX_RECORD];
        control::Request {
            binding: identity.binding,
            epoch: identity.epoch,
            transaction,
            command: control::Command::Hello,
        }
        .encode(&mut record)
        .map_err(|_| Error::InvalidResponse)?;
        record[6..8].copy_from_slice(&15u16.to_le_bytes());
        record[8..12].copy_from_slice(&48u32.to_le_bytes());
        record[40..44].copy_from_slice(&(request.bytes() as u32).to_le_bytes());
        let mut wire = [0; 48];
        wire.copy_from_slice(&record[..48]);
        identity.last_transaction = transaction;
        self.phase = Phase::Sending(Transaction {
            wire,
            bytes: request.bytes(),
            deadline: request.deadline(),
            health: now.saturating_add(HEALTH_NS),
        });
        Ok(())
    }
    pub fn cancel(&mut self) {
        self.phase = match self.phase {
            Phase::Sending(_) => Phase::Idle,
            Phase::Awaiting(request) => Phase::Draining(request),
            _ => return,
        };
    }
    pub fn next_deadline(&self) -> u64 {
        match &self.phase {
            Phase::Sending(tx) | Phase::Awaiting(tx) => tx.health.min(tx.deadline),
            Phase::Draining(tx) => tx.health,
            _ => u64::MAX,
        }
    }
    pub fn interest(&self) -> Interest {
        match self.phase {
            Phase::Sending(_) => Interest::Write,
            Phase::Idle
            | Phase::Awaiting(_)
            | Phase::Draining(_)
            | Phase::Poisoned { drain: true } => Interest::Read,
            _ => Interest::None,
        }
    }
    pub fn take_fault(&mut self) -> bool {
        if matches!(self.phase, Phase::Poisoned { .. }) && !self.fault_reported {
            self.fault_reported = true;
            true
        } else {
            false
        }
    }
    fn fail(&mut self, drain: bool, reply: bool) -> hyper_os::Result<Step> {
        self.phase = Phase::Poisoned { drain };
        if reply {
            Ok(Step::Complete(Reply::error(Status::IO_ERROR)?))
        } else {
            Ok(Step::Progress)
        }
    }
    pub fn poll(&mut self, transport: &impl ControlTransport, now: u64) -> hyper_os::Result<Step> {
        if let Phase::Sending(tx) | Phase::Awaiting(tx) = self.phase
            && now >= tx.deadline
        {
            self.cancel();
            return Ok(Step::Complete(Reply::error(Status::TIMED_OUT)?));
        }
        match self.phase {
            Phase::Awaiting(tx) if now >= tx.health => return self.fail(true, true),
            Phase::Draining(tx) if now >= tx.health => return self.fail(true, false),
            _ => (),
        }
        match self.phase {
            Phase::Unsupported | Phase::Poisoned { drain: false } => Ok(Step::Waiting),
            Phase::Sending(tx) => {
                if now >= tx.health {
                    return self.fail(false, true);
                }
                match transport.send(&tx.wire) {
                    Ok(()) => {
                        self.phase = Phase::Awaiting(Transaction {
                            health: now.saturating_add(HEALTH_NS),
                            ..tx
                        });
                        Ok(Step::Progress)
                    }
                    Err(Error::Status(Status::WOULD_BLOCK)) => Ok(Step::Waiting),
                    Err(_) => self.fail(false, true),
                }
            }
            _ => {
                let mut record = Record([0; control::MAX_RECORD]);
                match transport.receive(&mut record.0) {
                    Err(Error::Status(Status::WOULD_BLOCK)) => Ok(Step::Waiting),
                    Err(_) => self.fail(false, matches!(self.phase, Phase::Awaiting(_))),
                    Ok(length) => match self.phase {
                        Phase::Poisoned { .. } => {
                            self.phase = Phase::Poisoned { drain: false };
                            Ok(Step::Progress)
                        }
                        Phase::Idle => self.fail(false, false),
                        Phase::Awaiting(tx) | Phase::Draining(tx) => {
                            let draining = matches!(self.phase, Phase::Draining(_));
                            let decoded =
                                record.0.get(..length).and_then(|bytes| decode(bytes, &tx));
                            let Some(result) = decoded else {
                                return self.fail(false, !draining);
                            };
                            self.phase = Phase::Idle;
                            if draining {
                                return Ok(Step::Progress);
                            }
                            Ok(Step::Complete(match result {
                                Ok(payload) => Reply::success(payload)?,
                                Err(status) => Reply::error(status)?,
                            }))
                        }
                        _ => Err(Error::InvalidResponse),
                    },
                }
            }
        }
    }
}

fn decode<'a>(bytes: &'a [u8], tx: &Transaction) -> Option<Result<&'a [u8], Status>> {
    if bytes.len() < 48
        || bytes[..8] != tx.wire[..8]
        || bytes[16..40] != tx.wire[16..40]
        || u32::from_le_bytes(bytes[8..12].try_into().ok()?) as usize != bytes.len()
        || u32::from_le_bytes(bytes[12..16].try_into().ok()?) != 1
    {
        return None;
    }
    let status = u32::from_le_bytes(bytes[40..44].try_into().ok()?);
    let count = u32::from_le_bytes(bytes[44..48].try_into().ok()?) as usize;
    if status == 0 {
        return (count == tx.bytes && count <= MAX_BYTES && bytes.len() == 48 + count)
            .then_some(Ok(&bytes[48..]));
    }
    if count != 0 || bytes.len() != 48 {
        return None;
    }
    Some(Err(match status {
        1 => Status::NOT_SUPPORTED,
        2 => Status::INVALID_ARGUMENT,
        3..=5 => Status::IO_ERROR,
        6 => Status::WOULD_BLOCK,
        _ => return None,
    }))
}

#[cfg(test)]
#[path = "../../tests/random_owner.rs"]
mod tests;
