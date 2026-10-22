// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Validate first, then receive all live ownership in one kernel rendezvous.

use super::broker::Broker;
use super::{Result, show};
use hyper_io_runtime_system::devices::{
    Devices,
    worker::{self, Worker},
};
use hyper_io_runtime_system::guest::install::InstalledGuest;
use hyper_io_runtime_system::guest_log::GuestLog;
use hyper_io_runtime_system::handoff::{self, Description, Kind, WorkerMode};
use hyper_os::capability_channel::{CapabilityChannel, CapabilityReceiveSlot};
use hyper_os::guest_io::{Interrupt, Mailbox};
use hyper_os::handle::*;
use hyper_os::memory::SnapshotVmo;
use hyper_os::startup::{Startup, StartupPurpose};
use hyper_os::virtual_serial::{Output, PausedOutput};
use std::mem::MaybeUninit;
use std::sync::Arc;

pub(super) struct Adopted {
    pub guest: InstalledGuest,
    pub log: GuestLog,
    pub broker: Option<Broker>,
    pub worker: Option<Worker>,
    // Owners outlive every guest access. No backing or physical assignment is
    // recreated by adoption; native filesystem traffic keeps its original path.
    pub physical: OwnedHandle<PhysicalDeviceObject>,
    pub network: Option<OwnedHandle<PhysicalDeviceObject>>,
    pub _native_memory: Vec<OwnedHandle<GuestMemoryObject>>,
    pub native_mailboxes: Vec<Mailbox>,
}

pub(super) fn receive(startup: &mut Startup<'_>) -> Result<Adopted> {
    let channel = CapabilityChannel::from_handle(
        startup
            .take(StartupPurpose::new(handoff::ENDPOINT))
            .map_err(|error| stage("take endpoint", error))?,
    );
    // The generic startup coordinator delegates only the migration endpoint.
    // This implementation, not bootstrap policy, owns the I/O VM wire format.
    let mut record = [MaybeUninit::uninit(); 64];
    let mut description_slot = [CapabilityReceiveSlot::new::<VmoObject>(
        Rights::READ.union(Rights::TRANSFER).union(Rights::INSPECT),
    )];
    let message = channel
        .receive(
            hyper_os::DEADLINE_INFINITE,
            &mut record,
            &mut description_slot,
        )
        .map_err(|error| stage("receive description", error))?;
    if message.bytes() != handoff::DESCRIBE_MESSAGE || message.capability_count() != 1 {
        return Err("invalid I/O description message".into());
    }
    let snapshot = description_slot[0]
        .take::<VmoObject>()
        .map_err(|error| stage("take description", error))?
        .ok_or("missing I/O description")?;
    let snapshot = SnapshotVmo::from_vmo(snapshot.as_handle_ref())
        .map_err(|error| stage("snapshot description", error))?;
    let mut length = [0; 8];
    snapshot
        .read_exact_at(0, &mut length)
        .map_err(|error| stage("read description length", error))?;
    let length = usize::try_from(u64::from_le_bytes(length)).map_err(show)?;
    if length > handoff::MAX_DESCRIPTION {
        return Err("I/O handoff description too large".into());
    }
    let mut bytes = vec![0; length];
    snapshot
        .read_exact_at(8, &mut bytes)
        .map_err(|error| stage("read description", error))?;
    let description: Description =
        serde_json::from_slice(&bytes).map_err(|error| stage("decode description", error))?;
    description
        .validate()
        .map_err(|error| stage("validate description", error))?;
    #[cfg(not(feature = "userspace-device-test"))]
    if description.worker == WorkerMode::VirtioTest {
        return Err("test device consumer unavailable".into());
    }
    let mut slots = description
        .slots
        .iter()
        .map(receive_slot)
        .collect::<Result<Vec<_>>>()?;
    // Populate the executable and dependency closure while the previous I/O
    // owner is still serving filesystem reads. This call must precede receive:
    // the rendezvous transfers authority and ends pre-commit rollback.
    hyper_os::memory::lock_current_memory(
        startup.borrow(hyper_os::startup::ROOT_VMAR).map_err(show)?,
    )
    .map_err(|error| stage("lock I/O runtime mappings", error))?;
    let message = channel
        .receive(hyper_os::DEADLINE_INFINITE, &mut record, &mut slots)
        .map_err(|error| stage("receive owners", error))?;
    if message.capability_count() != slots.len() {
        return Err("incomplete I/O capability inventory".into());
    }
    let (cursor, line_start, bridge) = handoff::decode_commit(message.bytes())
        .map_err(|error| stage("decode committed state", error))?;
    let mut slots = slots.into_iter();
    let machine = Arc::new(take::<VirtualMachineObject>(&mut slots)?);
    let mut cpus = Vec::new();
    for _ in 0..description.cpus {
        cpus.push(Arc::new(take::<VirtualCpuObject>(&mut slots)?));
    }
    let serial = take::<VirtualSerialObject>(&mut slots)?;
    let memory = take::<VmoObject>(&mut slots)?;
    let own_memory = take::<GuestMemoryObject>(&mut slots)?;
    let hotplug_irq = take::<GuestInterruptObject>(&mut slots)?;
    let physical = take::<PhysicalDeviceObject>(&mut slots)?;
    let network = if description.network {
        Some(take::<PhysicalDeviceObject>(&mut slots)?)
    } else {
        None
    };
    let broker_endpoint = if description.broker {
        Some(take::<CapabilityChannelObject>(&mut slots)?)
    } else {
        None
    };
    let mut native_memory = Vec::new();
    for _ in 0..description.native {
        native_memory.push(take::<GuestMemoryObject>(&mut slots)?);
    }
    let mut native_mailboxes = Vec::new();
    for _ in 0..description.native.max(1) {
        native_mailboxes.push(Mailbox::from_handle(take::<GuestMailboxObject>(
            &mut slots,
        )?));
    }
    let mut broker_mailboxes = Vec::new();
    for _ in 0..description.clients.len() {
        broker_mailboxes.push(Mailbox::from_handle(take::<GuestMailboxObject>(
            &mut slots,
        )?));
    }
    if slots.next().is_some() {
        return Err("excess I/O capability inventory".into());
    }
    let output = Output::resume(
        PausedOutput {
            serial,
            memory,
            cursor,
        },
        startup.borrow(hyper_os::startup::ROOT_VMAR).map_err(show)?,
        0xd000_0000,
    )
    .map_err(|failure| stage("resume console mapping", failure.error()))?;
    let guest = InstalledGuest {
        name: description.name,
        image: description.image,
        machine,
        cpus,
        output,
    };
    let console_irq = Interrupt::create(
        guest.machine.as_handle_ref(),
        hyper_vm_support::pcie::ENDPOINT_IRQ,
    )
    .map_err(|error| stage("create console interrupt", error))?;
    let devices = Devices::new(
        bridge,
        own_memory,
        description.memory_base,
        description.memory_size,
        hotplug_irq,
        console_irq.into_handle(),
    )
    .map_err(|error| stage("prepare userspace devices", error))?;
    let worker = Some(
        worker::resume(&physical, &guest, description.worker, devices)
            .map_err(|error| stage("resume I/O device consumer", error))?,
    );
    let broker = broker_endpoint
        .map(|endpoint| Broker::adopt(endpoint, description.clients, broker_mailboxes))
        .transpose()?;
    Ok(Adopted {
        guest,
        log: GuestLog::restore(line_start),
        broker,
        worker,
        physical,
        network,
        _native_memory: native_memory,
        native_mailboxes,
    })
}

fn receive_slot(slot: &handoff::Slot) -> Result<CapabilityReceiveSlot> {
    let rights = Rights::from_bits(slot.rights).ok_or("invalid I/O capability rights")?;
    if !rights.contains(Rights::TRANSFER) {
        return Err("I/O owner must remain transferable".into());
    }
    Ok(match slot.kind {
        Kind::Machine => CapabilityReceiveSlot::new::<VirtualMachineObject>(rights),
        Kind::Cpu => CapabilityReceiveSlot::new::<VirtualCpuObject>(rights),
        Kind::Serial => CapabilityReceiveSlot::new::<VirtualSerialObject>(rights),
        Kind::SerialBuffer => CapabilityReceiveSlot::new::<VmoObject>(rights),
        Kind::StorageDevice | Kind::NetworkDevice => {
            CapabilityReceiveSlot::new::<PhysicalDeviceObject>(rights)
        }
        Kind::Broker => CapabilityReceiveSlot::new::<CapabilityChannelObject>(rights),
        Kind::OwnMemory | Kind::NativeMemory => {
            CapabilityReceiveSlot::new::<GuestMemoryObject>(rights)
        }
        Kind::HotplugInterrupt => CapabilityReceiveSlot::new::<GuestInterruptObject>(rights),
        Kind::NativeMailbox | Kind::BrokerMailbox => {
            CapabilityReceiveSlot::new::<GuestMailboxObject>(rights)
        }
    })
}
fn take<T: TypedObject>(
    slots: &mut impl Iterator<Item = CapabilityReceiveSlot>,
) -> Result<OwnedHandle<T>> {
    slots
        .next()
        .ok_or("missing I/O capability")?
        .take::<T>()
        .map_err(show)?
        .ok_or("empty I/O capability".into())
}

fn stage(operation: &str, error: impl std::fmt::Debug) -> String {
    format!("handoff {operation}: {error:?}")
}
