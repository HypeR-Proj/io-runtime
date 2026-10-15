// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! One guest binding: bounded mailbox progress and ordered retirement.

use super::super::{Result, check_deadline, deadline, show};
use super::{BindingPhase, ClientBinding, ClientSlot, listener};
use hyper_io_runtime_system::broker_exchange::Pending;
use hyper_io_runtime_system::broker_exchange::Step;
use hyper_io_runtime_system::protocol::BackendOperation;
use hyper_io_runtime_system::protocol::control::{Command, MAX_RECORD, Reply, Request, Status};
use hyper_os::capability_channel::{CapabilityChannel, CapabilityDisposition};
#[cfg(feature = "broker-test")]
use hyper_os::guest_io::Mailbox;
use hyper_os::guest_io::Operation;
use hyper_os::handle::{CapabilityChannelObject, RightsOffer};
use hyper_os::vm;
use hyper_os::wait::{ObjectSignals, WaitItem};
use hyper_service::io;

pub(super) fn session_closed(session: &CapabilityChannel) -> bool {
    hyper_os::wait::wait_many(
        &[WaitItem::new(
            session.as_handle_ref(),
            ObjectSignals::<CapabilityChannelObject>::PEER_CLOSED,
        )],
        0,
    )
    .is_ok()
}
fn would_block(error: &hyper_os::Error) -> bool {
    *error == hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK)
}
impl ClientSlot {
    /// Advances one binding without waiting for a Linux mailbox reply.
    ///
    /// An outstanding transaction is serviced before retirement can reuse its
    /// mailbox. `true` requests another progress pass; `false` lets the broker
    /// wait for readiness or a deadline instead of spinning on this client.
    pub(super) fn service(&mut self) -> Result<bool> {
        let Some(binding) = self.binding.as_mut() else {
            return Ok(false);
        };
        if !binding.retiring && session_closed(&binding.session) {
            binding.retiring = true;
        }
        // An already-sent request must finish before RESET may reuse this
        // mailbox, including cancelled admission and peer death during ACTIVATE.
        if binding.pending.is_some() {
            return self.service_pending();
        }

        if binding.retiring
            && matches!(
                binding.phase,
                BindingPhase::Hello
                    | BindingPhase::Prepare
                    | BindingPhase::ReturnHandles
                    | BindingPhase::Active
            )
        {
            binding.phase = BindingPhase::Retire;
        }
        let command = match binding.phase {
            BindingPhase::Hello => Some(if binding.devices & io::DISK != 0 {
                Command::Hello
            } else {
                Command::NetworkHello
            }),
            BindingPhase::Prepare => Some(Command::Prepare {
                alias: 0,
                guest_base: binding.base,
                length: binding.length,
                mapping_token: binding.mapping.token(),
            }),
            BindingPhase::Reset => Some(if binding.devices & io::DISK != 0 {
                Command::Device(BackendOperation::Reset)
            } else {
                Command::NetworkDevice(BackendOperation::Reset)
            }),
            BindingPhase::ResetNetwork => Some(Command::NetworkDevice(BackendOperation::Reset)),
            BindingPhase::Release => Some(Command::Release),
            _ => None,
        };
        if let Some(command) = command {
            self.queue(command, None)?;
            return Ok(true);
        }
        match binding.phase {
            BindingPhase::ReturnHandles => binding.return_handles(self.policy.id),
            BindingPhase::Retire => binding.begin_retirement(),
            BindingPhase::TryRelease | BindingPhase::FinalRelease => binding.release_mapping(),
            BindingPhase::Disconnect => {
                let mut disconnected = Ok(());
                for notification in binding.notifications.iter().flatten() {
                    let result = notification.disconnect().map_err(show);
                    if disconnected.is_ok() {
                        disconnected = result;
                    }
                }
                disconnected?;
                self.binding = None;
                Ok(true)
            }
            BindingPhase::Active => self.service_requests(),
            _ => Err("invalid broker phase".into()),
        }
    }

    /// Finish a sent request before a cancelled binding can reuse its mailbox.
    fn service_pending(&mut self) -> Result<bool> {
        let binding = self.binding.as_mut().ok_or("missing binding")?;
        let pending = binding.pending.as_mut().ok_or("missing transaction")?;
        if binding.retiring
            && pending.can_cancel()
            && !matches!(
                binding.phase,
                BindingPhase::Reset | BindingPhase::ResetNetwork | BindingPhase::Release
            )
        {
            binding.pending = None;
            binding.phase = BindingPhase::Retire;
            return Ok(true);
        }
        let mailbox = self.mailbox.as_ref().ok_or("missing mailbox")?;
        let now = hyper_os::time::monotonic_now()
            .map_err(show)?
            .as_nanoseconds();
        #[cfg(feature = "broker-test")]
        let step = {
            let hold = self.hold_reply && binding.phase == BindingPhase::Hello;
            if hold && pending.sent && !self.held_announced {
                self.held_announced = true;
                println!(
                    "BROKER-TEST REPLY-HELD client=1 generation={}",
                    binding.identity
                );
            }
            pending.poll(&HeldReply { mailbox, hold }, now)?
        };
        #[cfg(not(feature = "broker-test"))]
        let step = pending.poll(mailbox, now)?;
        match step {
            Step::Sent => {
                #[cfg(feature = "broker-test")]
                if self.policy.id == 1 && binding.phase == BindingPhase::Hello {
                    println!(
                        "BROKER-TEST HELLO-SENT client=1 generation={}",
                        binding.identity
                    );
                }
                if matches!(pending.request.command, Command::Prepare { .. }) {
                    binding.prepare_sent = true;
                }
                Ok(true)
            }
            Step::Waiting => Ok(false),
            Step::Reply(mut record) => {
                let length = record.len();
                let reply = Reply::decode(&record[..length], pending.request).map_err(show)?;
                if let Some(original) = pending.original {
                    if !binding.retiring {
                        record[32..40].copy_from_slice(&original.to_le_bytes());
                        binding.reply = Some(record[..length].to_vec());
                        binding.limit = deadline(30)?;
                    }
                } else if reply.status != Status::Success {
                    if matches!(
                        binding.phase,
                        BindingPhase::Reset | BindingPhase::ResetNetwork | BindingPhase::Release
                    ) {
                        return Err("backend refused retirement".into());
                    }
                    binding.retiring = true;
                }
                binding.pending = None;
                binding.phase = match binding.phase {
                    BindingPhase::Hello => BindingPhase::Prepare,
                    BindingPhase::Prepare => {
                        binding.limit = deadline(30)?;
                        BindingPhase::ReturnHandles
                    }
                    BindingPhase::Reset if binding.devices == (io::DISK | io::NETWORK) => {
                        BindingPhase::ResetNetwork
                    }
                    BindingPhase::Reset | BindingPhase::ResetNetwork => BindingPhase::Release,
                    BindingPhase::Release => {
                        binding.limit = deadline(5)?;
                        BindingPhase::FinalRelease
                    }
                    phase => phase,
                };
                Ok(true)
            }
        }
    }

    fn service_requests(&mut self) -> Result<bool> {
        let binding = self.binding.as_mut().ok_or("missing binding")?;
        if let Some(reply) = &binding.reply {
            match binding.channel.as_byte_channel().try_send(reply) {
                Ok(()) => binding.reply = None,
                Err(error) if would_block(&error) && check_deadline(binding.limit).is_ok() => {
                    return Ok(false);
                }
                Err(_) => binding.retiring = true,
            }
            return Ok(true);
        }
        let mut bytes = [0; MAX_RECORD];
        match binding.channel.as_byte_channel().try_receive(&mut bytes) {
            Ok(16) if &bytes[..8] == b"HIONOT01" => {
                let endpoint = u32::from_le_bytes(bytes[12..16].try_into().map_err(show)?) as usize;
                let operation = match u32::from_le_bytes(bytes[8..12].try_into().map_err(show)?) {
                    0 => Some(Operation::Disable),
                    1 => Some(Operation::Enable),
                    2 => Some(Operation::RaiseConfigurationInterrupt),
                    _ => None,
                };
                if let Some(epoch) = operation.and_then(|operation| {
                    binding
                        .notifications
                        .get(endpoint)?
                        .as_ref()?
                        .control(operation)
                        .ok()
                }) {
                    binding.epochs[endpoint] = epoch;
                    let mut reply = b"HIONOTR1".to_vec();
                    reply.extend_from_slice(&epoch.to_le_bytes());
                    reply.extend_from_slice(&(endpoint as u32).to_le_bytes());
                    binding.reply = Some(reply);
                    binding.limit = deadline(30)?;
                } else {
                    binding.retiring = true;
                }
            }
            Ok(length) => {
                if let Ok(request) = Request::decode(&bytes[..length])
                    && hyper_io_runtime_system::clients::authorize_request(
                        request,
                        binding.identity,
                        binding.epochs[request.command.device_kind() as usize],
                    )
                    && binding.devices & (1 << request.command.device_kind() as u32) != 0
                {
                    self.queue(request.command, Some(request.transaction))?;
                } else {
                    binding.retiring = true;
                }
            }
            Err(error) if would_block(&error) => return Ok(false),
            Err(_) => binding.retiring = true,
        }
        Ok(true)
    }

    fn queue(&mut self, command: Command, original: Option<u64>) -> Result<()> {
        let binding = self.binding.as_mut().ok_or("missing binding")?;
        let request = Request {
            binding: binding.identity,
            epoch: binding.epochs[match command {
                Command::Prepare { .. } | Command::Release => binding.primary(),
                _ => command.device_kind() as usize,
            }],
            transaction: self.transaction,
            command,
        };
        self.transaction = self
            .transaction
            .checked_add(1)
            .ok_or("backend sequence exhausted")?;
        binding.pending = Some(Pending {
            request,
            original,
            sent: false,
            limit: deadline(60)?,
        });
        Ok(())
    }
}

impl ClientBinding {
    fn primary(&self) -> usize {
        usize::from(self.devices & io::DISK == 0)
    }

    fn return_handles(&mut self, client_id: u32) -> Result<bool> {
        #[cfg(not(feature = "broker-test"))]
        let _ = client_id;
        let message = io::Binding {
            generation: self.identity,
            devices: self.devices,
        }
        .encode()
        .ok_or("invalid binding")?;
        let result = self.session.try_send(
            &message,
            &mut [
                CapabilityDisposition::move_handle(
                    &mut self.machine,
                    RightsOffer::Exact(listener::MACHINE_RIGHTS),
                )
                .map_err(show)?,
                CapabilityDisposition::move_handle(
                    &mut self.remote,
                    RightsOffer::Exact(io::MAILBOX_RIGHTS),
                )
                .map_err(show)?,
            ],
        );
        match result {
            Ok(()) => {
                self.phase = BindingPhase::Active;
                #[cfg(feature = "broker-test")]
                println!(
                    "BROKER-TEST BOUND client={} generation={}",
                    client_id, self.identity
                );
            }
            Err(error) if would_block(&error) && check_deadline(self.limit).is_ok() => {
                return Ok(false);
            }
            Err(_) => self.retiring = true,
        }
        Ok(true)
    }

    fn begin_retirement(&mut self) -> Result<bool> {
        if let Some(machine) = &self.machine {
            let _ = vm::request_stop(machine.as_handle_ref());
        }
        self.reply = None;
        for (index, notification) in self.notifications.iter().enumerate() {
            if let Some(notification) = notification {
                self.epochs[index] = notification.control(Operation::Disable).map_err(show)?;
            }
        }
        self.limit = deadline(5)?;
        self.phase = BindingPhase::TryRelease;
        Ok(true)
    }

    /// Advances mapping retirement while retaining backing until release succeeds.
    ///
    /// A busy first release after PREPARE enters the backend reset/drain path.
    /// Transport contention instead schedules a bounded retry of the same
    /// release, without issuing another reset or dropping the mapping owner.
    fn release_mapping(&mut self) -> Result<bool> {
        if self.retry_at != 0 && check_deadline(self.retry_at).is_ok() {
            check_deadline(self.limit)?;
            return Ok(false);
        }
        match self.mapping.release() {
            Ok(()) => {
                self.retry_at = 0;
                self.phase = BindingPhase::Disconnect;
            }
            Err(hyper_os::Error::Status(hyper_os::Status::BUSY))
                if self.phase == BindingPhase::TryRelease && self.prepare_sent =>
            {
                self.retry_at = 0;
                self.phase = BindingPhase::Reset;
            }
            Err(error) if would_block(&error) => {
                check_deadline(self.limit)?;
                self.retry_at = hyper_os::time::deadline_after(std::time::Duration::from_millis(1))
                    .map_err(show)?
                    .as_raw();
                return Ok(false);
            }
            Err(error) => return Err(show(error)),
        }
        Ok(true)
    }
}

/// Fault injection below the production pending transaction: the request really
/// reaches Linux, but its response stays queued until another slot retires.
#[cfg(feature = "broker-test")]
struct HeldReply<'a> {
    mailbox: &'a Mailbox,
    hold: bool,
}
#[cfg(feature = "broker-test")]
impl hyper_io_runtime_system::broker_exchange::ControlTransport for HeldReply<'_> {
    fn send(&self, bytes: &[u8]) -> hyper_os::Result<()> {
        self.mailbox.send(bytes)
    }
    fn receive(&self, bytes: &mut [u8]) -> hyper_os::Result<usize> {
        if self.hold {
            Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK))
        } else {
            self.mailbox.receive(bytes)
        }
    }
}
