// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! One bounded mailbox transaction, driven by the supervisor's fair slot loop.
use crate::protocol::control::{MAX_RECORD, Reply, Request};

/// Low-frequency backend configuration transport; queue notifications bypass it.
pub trait ControlTransport {
    fn send(&self, record: &[u8]) -> hyper_os::Result<()>;
    fn receive(&self, record: &mut [u8]) -> hyper_os::Result<usize>;
}
impl ControlTransport for hyper_os::guest_io::Mailbox {
    fn send(&self, record: &[u8]) -> hyper_os::Result<()> {
        hyper_os::guest_io::Mailbox::send(self, record)
    }
    fn receive(&self, record: &mut [u8]) -> hyper_os::Result<usize> {
        hyper_os::guest_io::Mailbox::receive(self, record)
    }
}

pub struct Pending {
    pub request: Request,
    pub original: Option<u64>,
    pub sent: bool,
    pub limit: u64,
}
pub enum Step {
    Waiting,
    Sent,
    Reply(Vec<u8>),
}
impl Pending {
    /// Cancellation cannot revoke a message already visible to Linux. The
    /// caller must drain that reply before issuing RESET or releasing grants.
    pub fn can_cancel(&self) -> bool {
        !self.sent
    }
    pub fn poll(&mut self, mailbox: &impl ControlTransport, now: u64) -> Result<Step, String> {
        let mut record = [0; MAX_RECORD];
        if !self.sent {
            self.check_deadline(now)?;
            let length = self
                .request
                .encode(&mut record)
                .map_err(|error| format!("{error:?}"))?;
            match mailbox.send(&record[..length]) {
                Ok(()) => {
                    self.sent = true;
                    return Ok(Step::Sent);
                }
                Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK)) => {
                    return Ok(Step::Waiting);
                }
                Err(error) => return Err(format!("{error:?}")),
            }
        }
        match mailbox.receive(&mut record) {
            Ok(length) => {
                let record = record
                    .get(..length)
                    .ok_or("oversized backend control reply")?;
                Reply::decode(record, self.request).map_err(|error| format!("{error:?}"))?;
                Ok(Step::Reply(record.to_vec()))
            }
            Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK)) => {
                self.check_deadline(now)?;
                Ok(Step::Waiting)
            }
            Err(error) => Err(format!("{error:?}")),
        }
    }
    fn check_deadline(&self, now: u64) -> Result<(), String> {
        if now >= self.limit {
            Err("backend control transaction timed out".into())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "../tests/broker_exchange.rs"]
mod tests;
