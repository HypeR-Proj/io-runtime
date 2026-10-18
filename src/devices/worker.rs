// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Independently scheduled MMIO/IRQ service: the main I/O runtime may block
//! waiting for Linux storage without blocking Linux's physical device exits.

use crate::guest::install::{InstalledGuest, PHYSICAL_MMIO};
use hyper_os::handle::{
    ByteChannelObject, OwnedHandle, PhysicalDeviceObject, VirtualCpuObject, VirtualMachineObject,
};
use hyper_os::wait::{ObjectSignals, RegistrationId, WaitSet};
use hyper_os::{Error, Result, Status, channel, device, vm};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::{self, JoinHandle};

use super::Devices;
use crate::handoff::WorkerMode;
use crate::sdhci::registers::{line_asserted, request_offset};

pub struct Worker {
    // Closing this endpoint is a durable stop notification, even if the worker
    // is between servicing a request and entering WaitSet::wait.
    stop: Option<OwnedHandle<ByteChannelObject>>,
    thread: Option<JoinHandle<Result<Exit>>>,
    connections: SyncSender<super::port::Port>,
}
impl Worker {
    /// Replace the sole internal serial consumer. A bounded mailbox and its
    /// durable wake byte keep reconnection out of physical-device servicing.
    pub fn connect_console(&self) -> Result<super::port::HostPort> {
        let (host, port) = super::port::HostPort::pair()?;
        self.connections
            .try_send(port)
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => Error::Status(Status::BUSY),
                mpsc::TrySendError::Disconnected(_) => Error::Status(Status::PEER_CLOSED),
            })?;
        let wake = self
            .stop
            .as_ref()
            .ok_or(Error::Status(Status::PEER_CLOSED))?;
        match wake.as_byte_channel().try_send(b"connect") {
            Ok(()) | Err(Error::Status(Status::WOULD_BLOCK)) => Ok(host),
            Err(error) => Err(error),
        }
    }

    /// Stop accepting exits, request VM shutdown, and join the worker. No VM
    /// lifecycle lock may be held by the caller while joining this thread.
    pub fn stop(mut self) -> Result<()> {
        self.stop.take();
        match self.thread.take() {
            Some(thread) => {
                thread
                    .join()
                    .map_err(|_| Error::Status(Status::INTERNAL))??;
                Ok(())
            }
            None => Ok(()),
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        // Never block a VM retirement path in Drop. The worker retains all
        // capabilities until it observes closure and requests shutdown.
        self.stop.take();
    }
}

enum Exit {
    Stopped,
}

struct MachineGuard {
    machine: Arc<OwnedHandle<VirtualMachineObject>>,
}
impl Drop for MachineGuard {
    fn drop(&mut self) {
        let _ = vm::request_stop(self.machine.as_handle_ref());
    }
}

struct Pump {
    physical: Option<OwnedHandle<PhysicalDeviceObject>>,
    devices: Devices,
    _machine: MachineGuard,
    cpus: Vec<Arc<OwnedHandle<VirtualCpuObject>>>,
    control: OwnedHandle<ByteChannelObject>,
    waits: WaitSet,
    stop_id: RegistrationId,
    machine_id: RegistrationId,
    irq_id: Option<RegistrationId>,
    cpu_ids: Vec<RegistrationId>,
    host_id: Option<RegistrationId>,
    irq_armed: bool,
    mode: WorkerMode,
    connections: Receiver<super::port::Port>,
}

pub fn resume(
    physical: &OwnedHandle<PhysicalDeviceObject>,
    guest: &InstalledGuest,
    mode: WorkerMode,
    devices: Devices,
) -> Result<Worker> {
    if guest.cpus.is_empty() || guest.cpus.len() > 8 {
        return Err(Error::Status(Status::NOT_SUPPORTED));
    }
    let physical = match mode {
        WorkerMode::None => None,
        // Match the predecessor's register/IRQ-only worker authority. This is
        // handle attenuation, not isolation within the shared process table.
        _ => Some(
            physical.duplicate(
                hyper_os::Rights::READ
                    .union(hyper_os::Rights::WRITE)
                    .union(hyper_os::Rights::WAIT)
                    .union(hyper_os::Rights::ACK_INTERRUPT),
            )?,
        ),
    };
    // Share the same process-local capabilities. Installed VM/vCPU handles
    // intentionally cannot be duplicated through the Native capability API.
    let machine = MachineGuard {
        machine: Arc::clone(&guest.machine),
    };
    let mut cpus = Vec::new();
    cpus.try_reserve_exact(guest.cpus.len())
        .map_err(|_| Error::Status(Status::NO_MEMORY))?;
    for cpu in &guest.cpus {
        cpus.push(Arc::clone(cpu));
    }
    let (stop, receiver) = channel::create_pair()?;
    let (connections, requests) = mpsc::sync_channel(1);
    let waits = WaitSet::new(cpus.len() + 4)?;
    let stop_id = waits.add(
        receiver.as_handle_ref(),
        ObjectSignals::<ByteChannelObject>::PEER_CLOSED
            .union(ObjectSignals::<ByteChannelObject>::READABLE),
    )?;
    let machine_id = waits.add(
        machine.machine.as_handle_ref(),
        ObjectSignals::<VirtualMachineObject>::TERMINATED,
    )?;
    let irq_id = physical
        .as_ref()
        .map(|physical| {
            waits.add(
                physical.as_handle_ref(),
                ObjectSignals::<PhysicalDeviceObject>::READABLE,
            )
        })
        .transpose()?;
    let mut cpu_ids = Vec::new();
    cpu_ids
        .try_reserve_exact(cpus.len())
        .map_err(|_| Error::Status(Status::NO_MEMORY))?;
    for cpu in &cpus {
        cpu_ids.push(waits.add(
            cpu.as_handle_ref(),
            ObjectSignals::<VirtualCpuObject>::MMIO_REQUEST,
        )?);
    }
    let mut pump = Pump {
        physical,
        devices,
        _machine: machine,
        cpus,
        control: receiver,
        waits,
        stop_id,
        machine_id,
        irq_id,
        cpu_ids,
        host_id: None,
        irq_armed: true,
        mode,
        connections: requests,
    };
    let thread = thread::Builder::new()
        .name("io-devices".into())
        .stack_size(64 * 1024)
        .spawn(move || pump.service())
        .map_err(|_| Error::Status(Status::NO_MEMORY))?;
    Ok(Worker {
        stop: Some(stop),
        thread: Some(thread),
        connections,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Line {
    Asserted,
    Idle,
    Changed,
}

impl Pump {
    fn reconcile(&self) -> Result<Line> {
        for _ in 0..2 {
            let Some(physical) = &self.physical else {
                return Ok(Line::Idle);
            };
            let sequence = device::irq_pending(physical.as_handle_ref())?;
            let asserted = match self.mode {
                WorkerMode::Sdhci => {
                    let status = device::mmio_read(physical.as_handle_ref(), 0x30, 4)? as u32;
                    let enable = device::mmio_read(physical.as_handle_ref(), 0x38, 4)? as u32;
                    line_asserted(status, enable)
                }
                #[cfg(feature = "userspace-device-test")]
                WorkerMode::VirtioTest => {
                    device::mmio_read(physical.as_handle_ref(), 0x60, 4)? & 3 != 0
                }
                _ => return Err(Error::InvalidResponse),
            };
            match device::irq_complete(physical.as_handle_ref(), sequence, asserted) {
                Ok(()) => return Ok(if asserted { Line::Asserted } else { Line::Idle }),
                Err(Error::Status(Status::BUSY)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(Line::Changed)
    }

    fn update_irq(&mut self) -> Result<()> {
        let line = self.reconcile()?;
        if !self.irq_armed
            && line != Line::Asserted
            && let Some(irq_id) = self.irq_id
        {
            self.waits.rearm(irq_id)?;
            self.irq_armed = true;
        }
        Ok(())
    }

    fn mmio(&mut self, index: usize) -> Result<()> {
        if let Some(request) = vm::pending_mmio(self.cpus[index].as_handle_ref())? {
            let completion = if Devices::owns(&request) {
                self.devices.mmio(request)?
            } else {
                let physical = self.physical.as_ref().ok_or(Error::InvalidResponse)?;
                let offset = request_offset(request, PHYSICAL_MMIO)?;
                match request.operation {
                    vm::MmioOperation::Read => vm::MmioCompletion::Read(device::mmio_read(
                        physical.as_handle_ref(),
                        offset,
                        request.width,
                    )?),
                    vm::MmioOperation::Write(value) => {
                        device::mmio_write(physical.as_handle_ref(), offset, request.width, value)?;
                        vm::MmioCompletion::Write
                    }
                }
            };
            // Resample after every access, including partial-width W1C writes,
            // before allowing the trapped instruction to resume.
            self.update_irq()?;
            vm::complete_mmio(self.cpus[index].as_handle_ref(), request.id, completion)?;
        }
        self.waits.rearm(self.cpu_ids[index])
    }

    fn service(&mut self) -> Result<Exit> {
        self.update_irq()?;
        self.devices.insert()?;
        loop {
            // Budgeted queue work must not sleep waiting for another guest
            // kick after already consuming its notification.
            let more = self.devices.poll()?;
            if let Some(previous) = self.host_id.take() {
                self.waits.remove(previous)?;
            }
            if let Some(port) = self.devices.host_port() {
                self.host_id = Some(self.waits.add(port.as_handle_ref(), port.signals())?);
            }
            let limit = if more {
                hyper_os::time::monotonic_now()?.as_nanoseconds()
            } else {
                u64::MAX
            };
            let event = match self.waits.wait(limit) {
                Ok(event) => event,
                Err(Error::Status(Status::TIMED_OUT)) if more => continue,
                Err(error) => return Err(error),
            };
            if event.registration == self.machine_id {
                return Ok(Exit::Stopped);
            }
            if event.registration == self.stop_id {
                if ObjectSignals::<ByteChannelObject>::PEER_CLOSED.is_present_in(event.signals) {
                    return Ok(Exit::Stopped);
                }
                let mut message = [0; 16];
                match self.control.as_byte_channel().try_receive(&mut message) {
                    Ok(_) | Err(Error::Status(Status::WOULD_BLOCK)) => (),
                    Err(error) => return Err(error),
                }
                if let Ok(port) = self.connections.try_recv() {
                    self.devices.host_event(true);
                    if let Err(error) = self.devices.attach_port(port) {
                        // Telemetry attachment must not stop physical I/O.
                        eprintln!("HypeR io-runtime: agent port unavailable: {error:?}");
                    }
                }
                self.waits.rearm(self.stop_id)?;
                continue;
            }
            if Some(event.registration) == self.host_id {
                self.devices.host_event(
                    ObjectSignals::<ByteChannelObject>::PEER_CLOSED.is_present_in(event.signals),
                );
            } else if Some(event.registration) == self.irq_id {
                self.irq_armed = false;
                self.update_irq()?;
            } else if let Some(index) = self.cpu_ids.iter().position(|id| *id == event.registration)
            {
                self.mmio(index)?;
            } else {
                return Err(Error::InvalidResponse);
            }
        }
    }
}
