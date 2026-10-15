// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! One bounded capability-receive worker; the main thread owns every mapping.

use hyper_os::capability_channel::{CapabilityChannel, CapabilityReceiveSlot};
use hyper_os::handle::{
    ByteChannelObject, CapabilityChannelObject, GuestMemoryObject, OwnedHandle, Rights,
    VirtualMachineObject,
};
use hyper_service::io;
use std::mem::MaybeUninit;
use std::sync::mpsc::{self, Receiver};

pub(super) const MACHINE_RIGHTS: Rights = Rights::TRANSFER
    .union(Rights::WRITE)
    .union(Rights::WAIT)
    .union(Rights::INSPECT)
    .union(Rights::REQUEST_STOP);

pub(super) enum Request {
    Connect(Admission),
    Observe(CapabilityChannel),
}

pub(super) struct Admission {
    pub pci: bool,
    pub(super) client: u32,
    pub(super) volume: Option<String>,
    pub(super) network: Option<String>,
    pub(super) mac: [u8; 6],
    pub(super) session: CapabilityChannel,
    pub(super) machine: OwnedHandle<VirtualMachineObject>,
    pub(super) memory: OwnedHandle<GuestMemoryObject>,
    pub(super) base: u64,
    pub(super) length: u64,
}
pub(super) struct Listener {
    admissions: Receiver<hyper_os::Result<Option<Request>>>,
    pub(super) bell: OwnedHandle<ByteChannelObject>,
}
impl Listener {
    pub(super) fn start(endpoint: CapabilityChannel) -> hyper_os::Result<Self> {
        let (sender, admissions) = mpsc::sync_channel(1);
        let (bell, writer) = hyper_os::channel::create_pair()?;
        std::thread::Builder::new()
            .name("io-admit".into())
            .spawn(move || {
                loop {
                    let admission = receive(&endpoint);
                    let terminal = admission.is_err();
                    if sender.send(admission).is_err()
                        || writer.as_byte_channel().send(&[1]).is_err()
                    {
                        break;
                    }
                    if terminal {
                        break;
                    }
                }
            })
            .map_err(|_| hyper_os::Error::Status(hyper_os::Status::NO_MEMORY))?;
        Ok(Self { admissions, bell })
    }
    pub(super) fn receive(&self) -> hyper_os::Result<Option<Request>> {
        let mut byte = [0];
        if self.bell.as_byte_channel().try_receive(&mut byte)? != 1 || byte != [1] {
            return Err(hyper_os::Error::InvalidResponse);
        }
        self.admissions
            .try_recv()
            .map_err(|_| hyper_os::Error::InvalidResponse)?
    }
}
fn receive(endpoint: &CapabilityChannel) -> hyper_os::Result<Option<Request>> {
    let mut bytes = [MaybeUninit::uninit(); io::CONNECT_BYTES];
    let mut slots = [CapabilityReceiveSlot::new::<CapabilityChannelObject>(
        io::SESSION_RIGHTS,
    )];
    let message = endpoint.receive(hyper_os::DEADLINE_INFINITE, &mut bytes, &mut slots)?;
    if message.capability_count() != 1 {
        return Err(hyper_os::Error::InvalidResponse);
    }
    let session = CapabilityChannel::from_handle(
        slots[0]
            .take::<CapabilityChannelObject>()?
            .ok_or(hyper_os::Error::MissingHandle)?,
    );
    if message.bytes() == io::OBSERVE_MESSAGE {
        return Ok(Some(Request::Observe(session)));
    }
    let connection = io::decode_connect(message.bytes()).ok_or(hyper_os::Error::InvalidResponse)?;
    let client = connection.client;
    let volume = connection.volume.map(str::to_owned);
    let network = connection.network.map(str::to_owned);
    let mac = connection.mac;
    let admission = (|| -> hyper_os::Result<Admission> {
        let deadline = hyper_os::time::deadline_after(std::time::Duration::from_secs(60))?.as_raw();
        let mut bytes = [MaybeUninit::uninit(); io::MEMORY_BYTES];
        let mut slots = [
            CapabilityReceiveSlot::new::<VirtualMachineObject>(MACHINE_RIGHTS),
            CapabilityReceiveSlot::new::<GuestMemoryObject>(io::MEMORY_RIGHTS),
        ];
        let message = session.receive(deadline, &mut bytes, &mut slots)?;
        let (base, length) =
            io::decode_memory(message.bytes()).ok_or(hyper_os::Error::InvalidResponse)?;
        if message.capability_count() != 2 {
            return Err(hyper_os::Error::InvalidResponse);
        }
        Ok(Admission {
            pci: &message.bytes()[..8] == io::PCI_MEMORY_MESSAGE,
            client,
            volume,
            network,
            mac,
            session,
            base,
            length,
            machine: slots[0]
                .take::<VirtualMachineObject>()?
                .ok_or(hyper_os::Error::MissingHandle)?,
            memory: slots[1]
                .take::<GuestMemoryObject>()?
                .ok_or(hyper_os::Error::MissingHandle)?,
        })
    })();
    // A cancelled per-runtime setup owns no DMA mapping and cannot take down
    // the shared configuration volume or other already-bound clients.
    Ok(admission.ok().map(Request::Connect))
}
