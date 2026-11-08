// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! One persistent public receiver, bounded admission and private reply ownership.

use hyper_os::{
    Error, Status,
    capability_channel::{CapabilityChannel, CapabilityReceiveSlot},
    handle::{ByteChannelObject, OwnedHandle},
};
use hyper_service::random::{self, Request};
use std::{
    mem::MaybeUninit,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::Duration,
};

pub struct Admission {
    pub request: Request,
    pub reply: OwnedHandle<ByteChannelObject>,
}

pub struct Listener {
    pub bell: OwnedHandle<ByteChannelObject>,
    queue: Option<mpsc::Receiver<hyper_os::Result<Admission>>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}
impl Listener {
    pub fn start(endpoint: CapabilityChannel) -> hyper_os::Result<Self> {
        let (sender, queue) = mpsc::sync_channel(8);
        let (bell, writer) = hyper_os::channel::create_pair()?;
        let stop = Arc::new(AtomicBool::new(false));
        let canceled = stop.clone();
        let worker = std::thread::Builder::new()
            .name("random-admit".into())
            .spawn(move || {
                while !canceled.load(Ordering::Acquire) {
                    let result = receive(&endpoint);
                    match result {
                        Ok(None)
                        | Err(Error::ReceiveRejected(_))
                        | Err(Error::Status(Status::TIMED_OUT)) => continue,
                        other => {
                            let terminal = other.is_err();
                            let item = match other {
                                Ok(Some(item)) => Ok(item),
                                Err(error) => Err(error),
                                Ok(None) => continue,
                            };
                            // Dropping the receiver unblocks a full bounded queue
                            // before Drop joins this sole worker.
                            if sender.send(item).is_err()
                                || writer.as_byte_channel().try_send(&[1]).is_err()
                                || terminal
                            {
                                break;
                            }
                        }
                    }
                }
            })
            .map_err(|_| Error::Status(Status::NO_MEMORY))?;
        Ok(Self {
            bell,
            queue: Some(queue),
            stop,
            worker: Some(worker),
        })
    }
    pub fn receive(&self) -> hyper_os::Result<Option<Admission>> {
        let mut byte = [0];
        match self.bell.as_byte_channel().try_receive(&mut byte) {
            Err(Error::Status(Status::WOULD_BLOCK)) => return Ok(None),
            Ok(1) if byte == [1] => (),
            Err(error) => return Err(error),
            _ => return Err(Error::InvalidResponse),
        }
        self.queue
            .as_ref()
            .ok_or(Error::InvalidResponse)?
            .try_recv()
            .map_err(|_| Error::InvalidResponse)?
            .map(Some)
    }
}
impl Drop for Listener {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.queue.take();
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            eprintln!("HypeR random: admission worker terminated unexpectedly");
        }
    }
}
fn receive(endpoint: &CapabilityChannel) -> hyper_os::Result<Option<Admission>> {
    let until = hyper_os::time::deadline_after(Duration::from_millis(250))?.as_raw();
    let mut bytes = [MaybeUninit::uninit(); random::REQUEST_BYTES];
    let mut slots = [CapabilityReceiveSlot::new::<ByteChannelObject>(
        random::REPLY_RIGHTS,
    )];
    let message = endpoint.receive(until, &mut bytes, &mut slots)?;
    if message.capability_count() != 1 {
        return Ok(None);
    }
    let Some(reply) = slots[0].take::<ByteChannelObject>()? else {
        return Ok(None);
    };
    match Request::decode(message.bytes()) {
        Ok(request) => Ok(Some(Admission { request, reply })),
        Err(_) => {
            let response = random::Reply::error(Status::INVALID_ARGUMENT)?;
            // Never wait on an untrusted reply endpoint, including malformed requests.
            let _ = reply.as_byte_channel().try_send(response.as_bytes());
            Ok(None)
        }
    }
}
