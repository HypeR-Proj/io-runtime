// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Bounded concurrent handshakes; the main broker owns every DMA mapping.

use hyper_os::capability_channel::{CapabilityChannel, CapabilityReceiveSlot};
use hyper_os::handle::{
    ByteChannelObject, CapabilityChannelObject, GuestMemoryObject, OwnedHandle,
    VirtualMachineObject,
};
use hyper_os::{Error, Status};
use hyper_service::io;
use std::mem::MaybeUninit;
use std::sync::mpsc::{self, Receiver};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;

pub enum Request {
    Connect(Admission),
    Observe(CapabilityChannel),
}

pub struct Admission {
    pub pci: bool,
    pub client: u32,
    pub volume: Option<String>,
    pub network: Option<String>,
    pub mac: [u8; 6],
    pub session: CapabilityChannel,
    pub machine: OwnedHandle<VirtualMachineObject>,
    pub memory: OwnedHandle<GuestMemoryObject>,
    pub base: u64,
    pub length: u64,
}

const MAX_HANDSHAKES: usize = 8;
const POLL_SLICE: std::time::Duration = std::time::Duration::from_millis(250);
type ResultSender = mpsc::SyncSender<hyper_os::Result<Option<Request>>>;

struct Handshake {
    client: u32,
    volume: Option<String>,
    network: Option<String>,
    mac: [u8; 6],
    session: CapabilityChannel,
    limit: u64,
}
enum Incoming {
    Connect(Handshake),
    Observe(CapabilityChannel),
}

pub struct Listener {
    admissions: Option<Receiver<hyper_os::Result<Option<Request>>>>,
    pub bell: OwnedHandle<ByteChannelObject>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}
impl Listener {
    pub fn start(endpoint: CapabilityChannel) -> hyper_os::Result<Self> {
        let (sender, admissions) = mpsc::sync_channel(MAX_HANDSHAKES);
        let (bell, writer) = hyper_os::channel::create_pair()?;
        let writer = Arc::new(writer);
        let stop = Arc::new(AtomicBool::new(false));
        let canceled = stop.clone();
        let worker = std::thread::Builder::new()
            .name("io-admit".into())
            .spawn(move || {
                let mut workers: Vec<JoinHandle<()>> = Vec::new();
                while !canceled.load(Ordering::Acquire) {
                    if let Err(error) = reap(&mut workers) {
                        publish(&sender, &writer, Err(error));
                        break;
                    }
                    let incoming = match receive_public(&endpoint) {
                        Ok(Some(incoming)) => incoming,
                        Ok(None) | Err(Error::Status(Status::TIMED_OUT)) => continue,
                        Err(error) => {
                            publish(&sender, &writer, Err(error));
                            break;
                        }
                    };
                    match incoming {
                        Incoming::Observe(session) => {
                            if !publish(&sender, &writer, Ok(Some(Request::Observe(session)))) {
                                break;
                            }
                        }
                        Incoming::Connect(handshake) => {
                            if let Err(error) = reap(&mut workers) {
                                publish(&sender, &writer, Err(error));
                                break;
                            }
                            // No mapping is published at this stage. Closing the
                            // private session is bounded refusal under overload.
                            if workers.len() == MAX_HANDSHAKES {
                                continue;
                            }
                            let sender = sender.clone();
                            let writer = writer.clone();
                            let canceled = canceled.clone();
                            match std::thread::Builder::new()
                                .name("io-memory-admit".into())
                                .spawn(move || {
                                    let result = receive_memory(handshake, &canceled);
                                    if !canceled.load(Ordering::Acquire) {
                                        publish(&sender, &writer, result);
                                    }
                                }) {
                                Ok(worker) => workers.push(worker),
                                Err(error) => eprintln!("io admission worker unavailable: {error}"),
                            }
                        }
                    }
                }
                canceled.store(true, Ordering::Release);
                for worker in workers {
                    if worker.join().is_err() {
                        eprintln!("io admission worker terminated unexpectedly");
                    }
                }
            })
            .map_err(|_| Error::Status(Status::NO_MEMORY))?;
        Ok(Self {
            admissions: Some(admissions),
            bell,
            stop,
            worker: Some(worker),
        })
    }
    pub fn receive(&self) -> hyper_os::Result<Option<Request>> {
        let mut byte = [0];
        if self.bell.as_byte_channel().try_receive(&mut byte)? != 1 || byte != [1] {
            return Err(Error::InvalidResponse);
        }
        self.admissions
            .as_ref()
            .ok_or(Error::InvalidResponse)?
            .try_recv()
            .map_err(|_| Error::InvalidResponse)?
    }
}
impl Drop for Listener {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.admissions.take();
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            eprintln!("io admission owner terminated unexpectedly");
        }
    }
}

fn publish(
    sender: &ResultSender,
    bell: &OwnedHandle<ByteChannelObject>,
    value: hyper_os::Result<Option<Request>>,
) -> bool {
    // At most one bell per bounded queued result; this pipe cannot accumulate
    // arbitrary notifications while the main broker is processing mappings.
    sender.send(value).is_ok() && bell.as_byte_channel().send(&[1]).is_ok()
}
fn reap(workers: &mut Vec<JoinHandle<()>>) -> hyper_os::Result<()> {
    let mut index = 0;
    while index < workers.len() {
        if workers[index].is_finished() {
            workers
                .swap_remove(index)
                .join()
                .map_err(|_| Error::Status(Status::INTERNAL))?;
        } else {
            index += 1;
        }
    }
    Ok(())
}
fn receive_public(endpoint: &CapabilityChannel) -> hyper_os::Result<Option<Incoming>> {
    let mut bytes = [MaybeUninit::uninit(); io::CONNECT_BYTES];
    let mut slots = [CapabilityReceiveSlot::new::<CapabilityChannelObject>(
        io::SESSION_RIGHTS,
    )];
    let until = hyper_os::time::deadline_after(POLL_SLICE)?.as_raw();
    let message = match endpoint.receive(until, &mut bytes, &mut slots) {
        Ok(message) => message,
        Err(Error::ReceiveRejected(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    if message.capability_count() != 1 {
        return Ok(None);
    }
    let session = CapabilityChannel::from_handle(slots[0].take()?.ok_or(Error::MissingHandle)?);
    if message.bytes() == io::OBSERVE_MESSAGE {
        return Ok(Some(Incoming::Observe(session)));
    }
    let Some(connection) = io::decode_connect(message.bytes()) else {
        return Ok(None);
    };
    Ok(Some(Incoming::Connect(Handshake {
        client: connection.client,
        volume: connection.volume.map(str::to_owned),
        network: connection.network.map(str::to_owned),
        mac: connection.mac,
        session,
        limit: hyper_os::time::deadline_after(std::time::Duration::from_secs(60))?.as_raw(),
    })))
}
fn receive_memory(
    handshake: Handshake,
    canceled: &AtomicBool,
) -> hyper_os::Result<Option<Request>> {
    let Handshake {
        client,
        volume,
        network,
        mac,
        session,
        limit,
    } = handshake;
    let mut bytes = [MaybeUninit::uninit(); io::MEMORY_BYTES];
    let mut slots = [
        CapabilityReceiveSlot::new::<VirtualMachineObject>(io::MACHINE_RIGHTS),
        CapabilityReceiveSlot::new::<GuestMemoryObject>(io::MEMORY_RIGHTS),
    ];
    while !canceled.load(Ordering::Acquire) {
        if hyper_os::time::monotonic_now()?.as_nanoseconds() >= limit {
            return Ok(None);
        }
        let until = hyper_os::time::deadline_after(POLL_SLICE)?
            .as_raw()
            .min(limit);
        let message = match session.receive(until, &mut bytes, &mut slots) {
            Ok(message) => message,
            Err(Error::Status(Status::TIMED_OUT)) => continue,
            Err(Error::ReceiveRejected(_))
            | Err(Error::Status(Status::PEER_CLOSED | Status::CANCELLED)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some((base, length)) = io::decode_memory(message.bytes()) else {
            return Ok(None);
        };
        if message.capability_count() != 2 {
            return Ok(None);
        }
        return Ok(Some(Request::Connect(Admission {
            pci: &message.bytes()[..8] == io::PCI_MEMORY_MESSAGE,
            client,
            volume,
            network,
            mac,
            session,
            base,
            length,
            machine: slots[0].take()?.ok_or(Error::MissingHandle)?,
            memory: slots[1].take()?.ok_or(Error::MissingHandle)?,
        })));
    }
    Ok(None)
}
