// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Board-scoped ownership of mailbox control and dynamic guest mappings.

mod client;
mod listener;

use client::session_closed;

use super::{Result, check_deadline, deadline, show};
use hyper_io_runtime_system::broker_exchange::Pending;
use hyper_io_runtime_system::guest::install::InstalledGuest;
use hyper_os::capability_channel::CapabilityChannel;
use hyper_os::guest_io::{Mailbox, Notification};
use hyper_os::handle::{
    ByteChannelObject, CapabilityChannelObject, GuestMailboxObject, OwnedHandle,
    VirtualMachineObject,
};
use hyper_os::vm;
use hyper_os::wait::{ObjectSignals, WaitItem};
use hyper_service::io;
use hyper_vm_image::guest_fdt::io::{IoClient, MmioDevice, SharedMemory};
use std::num::NonZeroU64;

pub(super) struct Broker {
    listener: Option<listener::Listener>,
    slots: Vec<ClientSlot>,
    observations: Vec<(CapabilityChannel, u64)>,
    #[cfg(feature = "broker-test")]
    fast_released: bool,
    #[cfg(feature = "broker-test")]
    fast_was_bound: bool,
}
struct ClientSlot {
    policy: hyper_io_runtime_system::clients::Client,
    mailbox: Option<Mailbox>,
    binding: Option<ClientBinding>,
    transaction: u64,
    generation: u64,
    queued: Option<listener::Admission>,
    #[cfg(feature = "broker-test")]
    hold_reply: bool,
    #[cfg(feature = "broker-test")]
    held_announced: bool,
}
struct ClientBinding {
    session: CapabilityChannel,
    identity: u64,
    channel: OwnedHandle<ByteChannelObject>,
    mapping: vm::GuestMapping,
    notifications: [Option<Notification>; 2],
    devices: u32,
    epochs: [u32; 2],
    reply: Option<Vec<u8>>,
    prepare_sent: bool,
    phase: BindingPhase,
    pending: Option<Pending>,
    retiring: bool,
    machine: Option<OwnedHandle<VirtualMachineObject>>,
    remote: Option<OwnedHandle<ByteChannelObject>>,
    base: u64,
    length: u64,
    limit: u64,
    retry_at: u64,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum BindingPhase {
    Hello,
    Prepare,
    ReturnHandles,
    Active,
    Retire,
    TryRelease,
    Reset,
    ResetNetwork,
    Release,
    FinalRelease,
    Disconnect,
}

impl Broker {
    pub(super) fn adopt(
        endpoint: OwnedHandle<CapabilityChannelObject>,
        clients: Vec<hyper_io_runtime_system::clients::Client>,
        mailboxes: Vec<Mailbox>,
    ) -> Result<Self> {
        if clients.len() != mailboxes.len() {
            return Err("I/O client and mailbox inventory differ".into());
        }
        Ok(Self {
            observations: Vec::new(),
            #[cfg(feature = "broker-test")]
            fast_released: false,
            #[cfg(feature = "broker-test")]
            fast_was_bound: false,
            listener: Some(
                listener::Listener::start(CapabilityChannel::from_handle(endpoint))
                    .map_err(show)?,
            ),
            slots: clients
                .into_iter()
                .zip(mailboxes)
                .map(|(policy, mailbox)| ClientSlot {
                    policy,
                    mailbox: Some(mailbox),
                    binding: None,
                    transaction: 1,
                    generation: 0,
                    queued: None,
                    #[cfg(feature = "broker-test")]
                    hold_reply: false,
                    #[cfg(feature = "broker-test")]
                    held_announced: false,
                })
                .collect(),
        })
    }
    pub(super) fn descriptions(&self) -> Vec<IoClient> {
        self.slots
            .iter()
            .map(|slot| {
                let index = slot.policy.id;
                IoClient {
                    id: slot.policy.id,
                    dynamic: true,
                    shared_memory: SharedMemory {
                        base: vm::DYNAMIC_ALIAS_OFFSET,
                        size: vm::DYNAMIC_PHYSICAL_LIMIT,
                        guest_base: 0,
                    },
                    mailbox: MmioDevice {
                        base: 0x0a01_0000 + u64::from(index) * 0x20000,
                        size: 4096,
                        irq: 41 + index * 2,
                    },
                    notification: slot.policy.volume.as_ref().map(|_| MmioDevice {
                        base: 0x0a02_0000 + u64::from(index) * 0x20000,
                        size: 4096,
                        irq: 42 + index * 2,
                    }),
                    network_notification: slot.policy.network.as_ref().map(|_| MmioDevice {
                        base: io::NETWORK_NOTIFICATION_BASE
                            + u64::from(index) * io::NETWORK_NOTIFICATION_STRIDE,
                        size: 4096,
                        irq: io::NETWORK_NOTIFICATION_IRQ_BASE + index,
                    }),
                }
            })
            .collect()
    }
    pub(super) fn wait_items(&self) -> Vec<WaitItem<'_>> {
        let mut items = Vec::new();
        for (endpoint, _) in &self.observations {
            items.push(WaitItem::new(
                endpoint.as_handle_ref(),
                ObjectSignals::<CapabilityChannelObject>::PEER_RECEIVING
                    .union(ObjectSignals::<CapabilityChannelObject>::PEER_CLOSED),
            ));
        }

        if let Some(listener) = &self.listener {
            items.push(WaitItem::new(
                listener.bell.as_handle_ref(),
                ObjectSignals::<ByteChannelObject>::READABLE
                    .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
            ));
        }
        for slot in &self.slots {
            let Some(binding) = &slot.binding else {
                continue;
            };
            // Once closure is observed it must not remain permanently ready in
            // our wait set while the independent backend drains.
            if !binding.retiring {
                let mut signals = ObjectSignals::<CapabilityChannelObject>::PEER_CLOSED;
                if binding.phase == BindingPhase::ReturnHandles {
                    signals =
                        signals.union(ObjectSignals::<CapabilityChannelObject>::PEER_RECEIVING);
                }
                items.push(WaitItem::new(binding.session.as_handle_ref(), signals));
            }
            if let Some(pending) = &binding.pending {
                let signals = if pending.sent {
                    ObjectSignals::<GuestMailboxObject>::READABLE
                } else {
                    ObjectSignals::<GuestMailboxObject>::WRITABLE
                };
                #[cfg(feature = "broker-test")]
                if slot.hold_reply && pending.sent && binding.phase == BindingPhase::Hello {
                    continue;
                }
                if let Some(mailbox) = &slot.mailbox {
                    items.push(WaitItem::new(
                        mailbox.as_handle_ref(),
                        signals.union(ObjectSignals::<GuestMailboxObject>::PEER_CLOSED),
                    ));
                }
            } else if binding.phase == BindingPhase::Active {
                let signals = if binding.reply.is_some() {
                    ObjectSignals::<ByteChannelObject>::WRITABLE
                } else {
                    ObjectSignals::<ByteChannelObject>::READABLE
                };
                items.push(WaitItem::new(
                    binding.channel.as_handle_ref(),
                    signals.union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
                ));
            }
        }
        items
    }
    pub(super) fn next_deadline(&self) -> u64 {
        self.slots
            .iter()
            .filter_map(|slot| slot.binding.as_ref())
            .map(|binding| {
                if let Some(pending) = &binding.pending {
                    pending.limit
                } else if binding.retry_at != 0 {
                    binding.retry_at.min(binding.limit)
                } else if matches!(binding.phase, BindingPhase::ReturnHandles)
                    || binding.reply.is_some()
                {
                    binding.limit
                } else {
                    hyper_os::DEADLINE_INFINITE
                }
            })
            .chain(self.observations.iter().map(|(_, limit)| *limit))
            .min()
            .unwrap_or(hyper_os::DEADLINE_INFINITE)
    }
    /// One bounded turn for each client; no client owns the supervisor thread.
    pub(super) fn service(
        &mut self,
        guest: &mut InstalledGuest,
        metrics: io::metrics::GuestMetrics,
    ) -> Result<bool> {
        use hyper_io_runtime_system::admission_policy::{self, Poll};
        let mut progress = false;
        match admission_policy::poll(&mut self.listener, listener::Listener::receive) {
            Poll::Received(admission) => {
                match admission {
                    listener::Request::Connect(admission) => self.admit(guest, admission)?,
                    listener::Request::Observe(endpoint) => {
                        if self.observations.len() < 4 {
                            self.observations
                                .push((endpoint, deadline(io::OBSERVATION_TIMEOUT_SECONDS)?));
                        }
                    }
                }
                progress = true;
            }
            Poll::Idle => {}
            Poll::Disabled(error) => eprintln!(
                "HypeR io-runtime: new client admissions disabled: {}",
                show(error)
            ),
        }
        if !self.observations.is_empty() {
            match Self::observe(guest, metrics) {
                Ok(snapshot) => {
                    self.observations.retain(|(endpoint, limit)| {
                        let keep = check_deadline(*limit).is_ok()
                            && matches!(
                                endpoint.try_send(&snapshot, &mut []),
                                Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK))
                            );
                        progress |= !keep;
                        keep
                    });
                }
                Err(error) => {
                    // Observation failure must never tear down the data plane.
                    eprintln!("HypeR io-runtime: status unavailable: {error}");
                    self.observations.clear();
                    progress = true;
                }
            }
        }
        for slot in &mut self.slots {
            #[cfg(feature = "broker-test")]
            {
                slot.hold_reply = slot.policy.id == 1 && !self.fast_released;
            }
            progress |= slot.service()?;
            #[cfg(feature = "broker-test")]
            if slot.policy.id == 2 {
                if slot
                    .binding
                    .as_ref()
                    .is_some_and(|binding| binding.phase == BindingPhase::Active)
                {
                    self.fast_was_bound = true;
                }
                if self.fast_was_bound && !self.fast_released && slot.binding.is_none() {
                    self.fast_released = true;
                    println!(
                        "BROKER-TEST RELEASED client=2 generation={}",
                        slot.generation
                    );
                }
            }
        }
        for index in 0..self.slots.len() {
            if self.slots[index].binding.is_none()
                && let Some(admission) = self.slots[index].queued.take()
            {
                self.admit(guest, admission)?;
                progress = true;
            }
        }
        Ok(progress)
    }

    fn observe(
        guest: &InstalledGuest,
        metrics: io::metrics::GuestMetrics,
    ) -> hyper_os::Result<[u8; io::OBSERVATION_BYTES]> {
        let info = vm::machine_info(guest.machine.as_handle_ref())?;
        let boot = guest
            .cpus
            .first()
            .and_then(|cpu| vm::vcpu_info(cpu.as_handle_ref()).ok());
        io::encode_observation(
            guest.name(),
            guest.image(),
            info,
            hyper_io_runtime_system::guest::install::RAM_BYTES,
            boot.and_then(|info| info.host_cpu),
            boot.map(|info| info.scheduler_thread_id),
            metrics,
        )
        .ok_or(hyper_os::Error::InvalidResponse)
    }

    fn admit(&mut self, guest: &mut InstalledGuest, admission: listener::Admission) -> Result<()> {
        let descriptions = self.descriptions();
        let Some(index) = self.slots.iter().position(|slot| {
            slot.policy.id == admission.client
                && slot.policy.volume == admission.volume
                && slot.policy.network == admission.network
                && slot.policy.mac == admission.mac
        }) else {
            eprintln!(
                "HypeR io-runtime: rejected client {}: requested volume {:?}, network {:?}, MAC {:02x?} does not match the board I/O policy; check the VM configuration in /data/vms.json",
                admission.client, admission.volume, admission.network, admission.mac
            );
            let _ = vm::request_stop(admission.machine.as_handle_ref());
            return Ok(());
        };
        let slot = &mut self.slots[index];
        if let Some(binding) = slot.binding.as_mut() {
            if session_closed(&binding.session) && slot.queued.is_none() {
                binding.retiring = true;
                slot.queued = Some(admission);
            } else {
                let _ = vm::request_stop(admission.machine.as_handle_ref());
            }
            return Ok(());
        }
        let Some(generation) = slot
            .generation
            .checked_add(1)
            .filter(|value| *value < io::NETWORK_DEVICE_ID_BIT && slot.binding.is_none())
        else {
            let _ = vm::request_stop(admission.machine.as_handle_ref());
            return Ok(());
        };
        let identity = NonZeroU64::new(generation).ok_or("invalid binding generation")?;
        let description = descriptions[index];
        // Allocate the reply channel before creating routes or exposing a token.
        let mut notifications: [Option<Notification>; 2] = [None, None];
        let mut stage = "create reply channel";
        let resources = (|| -> hyper_os::Result<_> {
            let channels = hyper_os::channel::create_pair()?;
            if admission.pci {
                vm::register_mmio(
                    admission.machine.as_handle_ref(),
                    io::PCI_ECAM,
                    io::PCI_ECAM_BYTES,
                    NonZeroU64::new(io::PCI_ECAM_ID).ok_or(hyper_os::Error::InvalidResponse)?,
                )?;
                vm::register_mmio(
                    admission.machine.as_handle_ref(),
                    io::PCI_MEMORY,
                    io::PCI_MEMORY_BYTES,
                    NonZeroU64::new(io::PCI_MEMORY_ID).ok_or(hyper_os::Error::InvalidResponse)?,
                )?;
            }
            for (index, endpoint) in [description.notification, description.network_notification]
                .into_iter()
                .enumerate()
            {
                let Some(endpoint) = endpoint else {
                    continue;
                };
                let (base, irq, cookie) = if index == 0 {
                    (io::FRONTEND_MMIO, io::FRONTEND_IRQ, identity)
                } else {
                    (
                        io::FRONTEND_NET_MMIO,
                        io::FRONTEND_NET_IRQ,
                        NonZeroU64::new(identity.get() | io::NETWORK_DEVICE_ID_BIT)
                            .ok_or(hyper_os::Error::InvalidResponse)?,
                    )
                };
                stage = "register frontend MMIO";
                if !admission.pci {
                    vm::register_mmio(admission.machine.as_handle_ref(), base, 4096, cookie)?;
                }
                stage = "create notification route";
                notifications[index] = Some(Notification::create(
                    admission.machine.as_handle_ref(),
                    guest.machine.as_handle_ref(),
                    base,
                    endpoint.base,
                    irq,
                    endpoint.irq,
                )?);
            }
            if admission.pci {
                for (index, notification) in notifications.iter().enumerate() {
                    if notification.is_some() {
                        let base = if index == 0 {
                            io::FRONTEND_MMIO
                        } else {
                            io::FRONTEND_NET_MMIO
                        };
                        hyper_os::guest_io::configure_pci_frontend(
                            admission.machine.as_handle_ref(),
                            base,
                            base,
                            false,
                            false,
                            false,
                        )?;
                    }
                }
            }
            stage = "map backend guest memory";
            let mapping = vm::map_shared_guest_memory(
                guest.machine.as_handle_ref(),
                admission.memory.as_handle_ref(),
                admission.base,
            )?;
            Ok((channels, mapping))
        })();
        let ((channel, remote), mapping) = match resources {
            Ok(resources) => resources,
            Err(error) => {
                let _ = vm::request_stop(admission.machine.as_handle_ref());
                let mut disconnected = Ok(());
                for notification in notifications.into_iter().flatten() {
                    let result = notification.disconnect().map_err(show);
                    if disconnected.is_ok() {
                        disconnected = result;
                    }
                }
                disconnected?;
                eprintln!(
                    "HypeR io-runtime: rejected client {} setup ({stage}): {error}",
                    admission.client
                );
                return Ok(());
            }
        };
        slot.generation = generation;
        slot.binding = Some(ClientBinding {
            session: admission.session,
            identity: identity.get(),
            channel,
            mapping,
            notifications,
            devices: slot.policy.devices(),
            epochs: [1; 2],
            reply: None,
            prepare_sent: false,
            phase: BindingPhase::Hello,
            pending: None,
            retiring: false,
            machine: Some(admission.machine),
            remote: Some(remote),
            base: admission.base,
            length: admission.length,
            limit: deadline(30)?,
            retry_at: 0,
        });
        Ok(())
    }
}
