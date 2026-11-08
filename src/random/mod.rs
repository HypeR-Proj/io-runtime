// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Initialized random bytes from the trusted I/O VM, without kernel RNG policy.

mod listener;
pub mod owner;

use crate::handoff::OwnerControl;
use hyper_os::{
    Error, Status,
    capability_channel::CapabilityChannel,
    guest_io::Mailbox,
    handle::{ByteChannelObject, GuestMailboxObject},
    wait::{self, ObjectSignals, WaitItem},
};
use hyper_service::random::Reply;
use listener::{Admission, Listener};
use owner::{Interest, Owner, Step};

pub struct Service {
    listener: Option<Listener>,
    active: Option<Admission>,
    owner: Owner,
}
impl Service {
    pub fn new(
        endpoint: CapabilityChannel,
        control: Option<OwnerControl>,
    ) -> hyper_os::Result<Self> {
        Ok(Self {
            listener: Some(Listener::start(endpoint)?),
            active: None,
            owner: Owner::new(control),
        })
    }
    pub fn poll(&mut self, mailbox: &Mailbox, now: u64) -> hyper_os::Result<bool> {
        let mut progress = false;
        if let Some(active) = &self.active {
            match wait::wait_many(
                &[WaitItem::new(
                    active.reply.as_handle_ref(),
                    ObjectSignals::<ByteChannelObject>::PEER_CLOSED,
                )],
                0,
            ) {
                Ok(_) => {
                    self.owner.cancel();
                    self.active.take();
                    progress = true;
                }
                Err(Error::Status(Status::TIMED_OUT)) => (),
                Err(_) => {
                    self.owner.cancel();
                    self.active.take();
                    progress = true;
                }
            }
        }
        match self.owner.poll(mailbox, now)? {
            Step::Waiting => (),
            Step::Progress => progress = true,
            Step::Complete(reply) => {
                if let Some(active) = self.active.take() {
                    send(&active, &reply);
                }
                progress = true;
            }
        }
        if self.owner.take_fault() {
            eprintln!(
                "HypeR random: ERROR: owner control failed; random source disabled, established storage queues retained"
            );
        }
        if self.active.is_none() && self.owner.available() {
            let incoming = match &self.listener {
                Some(listener) => listener.receive(),
                None => return Ok(progress),
            };
            match incoming {
                Ok(Some(active)) => {
                    match self.owner.begin(active.request, now) {
                        Ok(()) => self.active = Some(active),
                        Err(error) => {
                            let status = match error {
                                Error::Status(
                                    status @ (Status::NOT_SUPPORTED
                                    | Status::TIMED_OUT
                                    | Status::IO_ERROR),
                                ) => status,
                                _ => Status::IO_ERROR,
                            };
                            send(&active, &Reply::error(status)?);
                        }
                    }
                    progress = true;
                }
                Ok(None) => (),
                Err(_) => {
                    eprintln!(
                        "HypeR random: ERROR: public listener closed; established storage queues retained"
                    );
                    self.listener.take();
                    progress = true;
                }
            }
        }
        Ok(progress)
    }
    pub fn next_deadline(&self) -> u64 {
        self.owner.next_deadline()
    }
    pub fn wait_items<'a>(&'a self, mailbox: &'a Mailbox) -> Vec<WaitItem<'a>> {
        let mut items = Vec::new();
        let signal = match self.owner.interest() {
            Interest::Read => Some(ObjectSignals::<GuestMailboxObject>::READABLE),
            Interest::Write => Some(ObjectSignals::<GuestMailboxObject>::WRITABLE),
            Interest::None => None,
        };
        if let Some(signal) = signal {
            items.push(WaitItem::new(
                mailbox.as_handle_ref(),
                signal.union(ObjectSignals::<GuestMailboxObject>::PEER_CLOSED),
            ));
        }
        if let Some(active) = &self.active {
            items.push(WaitItem::new(
                active.reply.as_handle_ref(),
                ObjectSignals::<ByteChannelObject>::PEER_CLOSED,
            ));
        } else if self.owner.available()
            && let Some(listener) = &self.listener
        {
            items.push(WaitItem::new(
                listener.bell.as_handle_ref(),
                ObjectSignals::<ByteChannelObject>::READABLE
                    .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
            ));
        }
        items
    }
}
fn send(active: &Admission, reply: &Reply) {
    // ByteChannel try_send is a nonblocking atomic message operation. A client
    // cannot suspend storage service by filling or abandoning its private pipe.
    let _ = active.reply.as_byte_channel().try_send(reply.as_bytes());
}
