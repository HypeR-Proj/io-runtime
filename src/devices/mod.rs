// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Formal userspace devices attached after taking ownership of the running VM.

mod memory;
pub mod port;
pub mod worker;

use hyper_os::guest_io::Interrupt;
use hyper_os::handle::{GuestInterruptObject, GuestMemoryObject, OwnedHandle};
use hyper_os::vm::{MmioCompletion, MmioOperation, MmioRequest};
use hyper_os::{Error, Result};
use hyper_vm_support::pcie::{self, RootPort};
use hyper_vm_support::{pci, virtio, virtio_console, virtio_pci};

const AGENT_PORT: &str = "org.qemu.guest_agent.0";

/// One event loop owns configuration, queues and interrupt levels. The RAM and
/// interrupt capabilities move with that owner; no borrowed guest pointer can
/// escape a kernel-checked copy operation.
pub struct Devices {
    port: Option<port::Port>,
    bridge: RootPort,
    device: virtio_pci::Device,
    console: virtio_console::Console,
    memory: memory::Ram,
    hotplug_irq: Interrupt,
    console_irq: Interrupt,
    isr: u8,
    active: bool,
    more: bool,
    failed: bool,
}

impl Devices {
    pub fn new(
        bridge: RootPort,
        memory: OwnedHandle<GuestMemoryObject>,
        memory_base: u64,
        memory_bytes: u64,
        hotplug_irq: OwnedHandle<GuestInterruptObject>,
        console_irq: OwnedHandle<GuestInterruptObject>,
    ) -> Result<Self> {
        if bridge.present() {
            return Err(Error::InvalidResponse);
        }
        let core = virtio::Device::console(memory_base, memory_bytes)
            .map_err(|_| Error::InvalidResponse)?;
        let console = virtio_console::Console::new(AGENT_PORT, memory_base, memory_bytes)
            .map_err(|_| Error::InvalidResponse)?;
        Ok(Self {
            port: None,
            bridge,
            device: virtio_pci::Device::new(core, pcie::MEMORY_BASE as u32),
            console,
            memory: memory::Ram {
                handle: memory,
                base: memory_base,
                bytes: memory_bytes,
            },
            hotplug_irq: Interrupt::from_handle(hotplug_irq),
            console_irq: Interrupt::from_handle(console_irq),
            isr: 0,
            active: false,
            more: false,
            failed: false,
        })
    }

    /// Attach one local byte-stream consumer from the device worker.
    /// A reset after activation invalidates this session; a new consumer must
    /// explicitly attach to the replacement device session.
    pub fn attach_port(&mut self, port: port::Port) -> Result<()> {
        if self.port.is_some() {
            return Err(Error::Status(hyper_os::Status::BUSY));
        }
        self.console
            .set_open(true)
            .map_err(|_| Error::InvalidResponse)?;
        self.port = Some(port);
        self.more = true;
        Ok(())
    }

    pub(super) fn host_port(&self) -> Option<&port::Port> {
        self.port.as_ref()
    }

    pub(super) fn host_event(&mut self, closed: bool) {
        if closed {
            self.port.take();
            if self.console.set_open(false).is_err() {
                self.fail_queue();
            }
        }
        self.more = true;
    }

    /// Publish presence only after the successor has started its MMIO consumer.
    pub fn insert(&mut self) -> Result<()> {
        self.bridge.set_present(true);
        self.update_interrupts()?;
        println!("HypeR io-runtime: virtio-console inserted on PCIe slot 1");
        Ok(())
    }

    pub fn owns(request: &MmioRequest) -> bool {
        matches!(
            request.device.get(),
            pcie::ECAM_COOKIE | pcie::MEMORY_COOKIE
        )
    }

    pub fn mmio(&mut self, request: MmioRequest) -> Result<MmioCompletion> {
        let width = u8::try_from(request.width).map_err(|_| Error::InvalidResponse)?;
        let absent = || match request.operation {
            MmioOperation::Read => MmioCompletion::Read(if width == 8 {
                u64::MAX
            } else {
                (1u64 << (8 * width)) - 1
            }),
            MmioOperation::Write(_) => MmioCompletion::Write,
        };
        let result = if let Some((bus, slot, function, offset)) = pcie::decode_ecam(request.address)
        {
            if (bus, slot, function) == (0, 0, 0) {
                let reset = self.bridge.secondary_reset();
                let result = match request.operation {
                    MmioOperation::Read => {
                        self.bridge.read(offset, width).map(MmioCompletion::Read)
                    }
                    MmioOperation::Write(value) => self
                        .bridge
                        .write(offset, width, value)
                        .map(|()| MmioCompletion::Write),
                };
                if !reset && self.bridge.secondary_reset() {
                    self.reset_endpoint()?;
                }
                if self.bridge.link_active() {
                    self.more = true;
                }
                result
            } else if self.bridge.routes_endpoint(bus, slot, function) {
                self.configuration(offset, width, request.operation).ok()
            } else {
                None
            }
        } else if self.bridge.forwards_memory(request.address, width) {
            let function = self.device.function();
            let base = function.bar();
            if function.command() & pci::MEMORY != 0
                && base >= pcie::MEMORY_BASE
                && base + pci::BAR_SIZE <= pcie::MEMORY_BASE + pcie::MEMORY_BYTES
                && request.address >= base
                && request
                    .address
                    .checked_add(u64::from(width))
                    .is_some_and(|end| end <= base + pci::BAR_SIZE)
            {
                self.register(request.address - base, width, request.operation)
                    .ok()
            } else {
                None
            }
        } else {
            None
        };
        self.update_interrupts()?;
        Ok(result.unwrap_or_else(absent))
    }

    fn configuration(
        &mut self,
        offset: u64,
        width: u8,
        operation: MmioOperation,
    ) -> std::result::Result<MmioCompletion, virtio::Error> {
        if let Some(window) = self.device.configuration_window(offset, width)? {
            let operation = match operation {
                MmioOperation::Read => MmioOperation::Read,
                MmioOperation::Write(value) => MmioOperation::Write(
                    self.device
                        .configuration_write_value(offset, width, value)?,
                ),
            };
            let completion = self.register(window.offset, window.width, operation)?;
            return match completion {
                MmioCompletion::Read(value) => Ok(MmioCompletion::Read(
                    self.device
                        .configuration_read_result(offset, width, value)?,
                )),
                other => Ok(other),
            };
        }
        match operation {
            MmioOperation::Read => self
                .device
                .read_configuration(offset, width, self.isr != 0)
                .map(MmioCompletion::Read),
            MmioOperation::Write(value) => {
                if let Some(transaction) = self.device.write_configuration(offset, width, value)? {
                    self.transaction(transaction)?;
                }
                Ok(MmioCompletion::Write)
            }
        }
    }

    fn register(
        &mut self,
        offset: u64,
        width: u8,
        operation: MmioOperation,
    ) -> std::result::Result<MmioCompletion, virtio::Error> {
        match (offset, width, operation) {
            (virtio_pci::ISR_OFFSET, 1, MmioOperation::Read) => Ok(MmioCompletion::Read(
                u64::from(std::mem::take(&mut self.isr)),
            )),
            (virtio_pci::NOTIFY_OFFSET, 2, MmioOperation::Write(queue)) if queue < 4 => {
                self.service_queues();
                Ok(MmioCompletion::Write)
            }
            (_, _, MmioOperation::Read) => {
                self.device.read(offset, width).map(MmioCompletion::Read)
            }
            (_, _, MmioOperation::Write(value)) => {
                if let Some(transaction) = self.device.write(offset, width, value)? {
                    self.transaction(transaction)?;
                }
                Ok(MmioCompletion::Write)
            }
        }
    }

    fn transaction(
        &mut self,
        transaction: virtio::Transaction,
    ) -> std::result::Result<(), virtio::Error> {
        let success = match transaction.operation {
            virtio::BackendOperation::Activate { features, queues } => {
                let success = self.console.activate(features, queues).is_ok();
                #[cfg(feature = "console-test")]
                let success = success && self.start_echo().is_ok();
                self.active = success;
                success
            }
            virtio::BackendOperation::Reset | virtio::BackendOperation::StopQueue { .. } => {
                let was_active = self.active;
                self.console.reset();
                if was_active {
                    self.port.take();
                    let _ = self.console.set_open(false);
                }
                self.active = false;
                self.failed = false;
                self.isr = 0;
                true
            }
        };
        self.device.complete(transaction.id, success)?;
        if !success
            || matches!(
                transaction.operation,
                virtio::BackendOperation::StopQueue { .. }
            )
        {
            self.isr |= 2;
        }
        Ok(())
    }

    fn reset_endpoint(&mut self) -> Result<()> {
        self.console.reset();
        if self.active {
            self.port.take();
            let _ = self.console.set_open(false);
        }
        self.device = virtio_pci::Device::new(
            virtio::Device::console(self.memory.base, self.memory.bytes)
                .map_err(|_| Error::InvalidResponse)?,
            pcie::MEMORY_BASE as u32,
        );
        self.active = false;
        self.failed = false;
        self.isr = 0;
        Ok(())
    }

    /// Returns whether an already-notified queue needs another bounded turn.
    pub fn poll(&mut self) -> Result<bool> {
        self.poll_host();
        if self.more {
            self.service_queues();
            self.poll_host();
        }
        self.update_interrupts()?;
        Ok(self.more)
    }

    fn poll_host(&mut self) {
        if self.console.take_guest_disconnect() {
            self.host_event(true);
        }
        if let Some(port) = &mut self.port {
            match port.poll(&mut self.console) {
                Ok(progress) => {
                    self.more |= progress.progress;
                    if progress.closed {
                        self.host_event(true);
                    }
                }
                Err(error) => {
                    eprintln!("HypeR io-runtime: console host session closed: {error:?}");
                    self.host_event(true);
                }
            }
        }
    }

    fn service_queues(&mut self) {
        self.more = false;
        if !self.active
            || self.failed
            || !self.bridge.link_active()
            || !self.bridge.bus_master_enabled()
            || self.device.function().command() & pci::BUS_MASTER == 0
        {
            return;
        }
        match self.console.service(&self.memory) {
            Ok(progress) => {
                self.more = progress.more;
                if progress.interrupt {
                    self.isr |= 1;
                }
            }
            Err(error) => {
                eprintln!("HypeR io-runtime: virtio-console queue rejected: {error:?}");
                self.fail_queue();
            }
        }
    }

    fn update_interrupts(&self) -> Result<()> {
        self.hotplug_irq.set_level(self.bridge.irq_asserted())?;
        self.console_irq.set_level(
            self.bridge.link_active()
                && self.isr != 0
                && self.device.function().command() & pci::INTX_DISABLE == 0,
        )
    }

    fn fail_queue(&mut self) {
        self.port.take();
        self.failed = true;
        self.device.needs_reset();
        self.isr |= 2;
    }

    #[cfg(feature = "console-test")]
    fn start_echo(&mut self) -> Result<()> {
        let (host, port) = port::HostPort::pair()?;
        self.attach_port(port)?;
        // Only the test consumer blocks. The device worker exercises the same
        // nonblocking adapter used by a real host-side serial client.
        std::thread::Builder::new()
            .name("console-test".into())
            .stack_size(32 * 1024)
            .spawn(move || {
                let mut bytes = [0; port::MAX_BYTES];
                let channel = host.channel();
                while let Ok(count) = channel.receive(&mut bytes) {
                    if channel.send(&bytes[..count]).is_err() {
                        break;
                    }
                }
            })
            .map_err(|_| Error::Status(hyper_os::Status::NO_MEMORY))?;
        Ok(())
    }
}
