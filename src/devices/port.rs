// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! A single host consumer for a virtio serial port, without an agent protocol.
//!
//! Channel messages carry raw byte chunks of 1..=4096 bytes. Message boundaries
//! are not preserved in the guest byte stream. The reactor owns `Port` and must
//! close it on guest reset after activation; an old `HostPort` then observes
//! `PEER_CLOSED` and cannot send bytes into a replacement guest session.

use hyper_os::channel::{self, ByteChannel};
use hyper_os::handle::{ByteChannelObject, HandleRef, OwnedHandle};
use hyper_os::wait::ObjectSignals;
use hyper_os::{Error, Result, Status};
use hyper_vm_support::virtio_console::Console;

pub const MAX_BYTES: usize = 4096;

/// Exclusively owned host endpoint. Attaching its peer to the device is an
/// explicit decision by the runtime; merely creating a pair does not open it.
pub struct HostPort {
    channel: OwnedHandle<ByteChannelObject>,
}

impl HostPort {
    pub fn pair() -> Result<(Self, Port)> {
        let (host, device) = channel::create_pair()?;
        Ok((
            Self { channel: host },
            Port {
                channel: device,
                buffers: Buffers::new(),
            },
        ))
    }

    pub fn as_handle_ref(&self) -> HandleRef<'_, ByteChannelObject> {
        self.channel.as_handle_ref()
    }

    #[cfg(feature = "console-test")]
    pub(super) fn channel(&self) -> ByteChannel<'_> {
        self.channel.as_byte_channel()
    }

    /// Sends a complete chunk or returns `WOULD_BLOCK` without consuming it.
    pub fn try_send(&self, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error::Status(Status::INVALID_ARGUMENT));
        }
        self.channel.as_byte_channel().try_send(bytes)
    }

    /// Receives one complete chunk. A 4096-byte buffer accepts every valid
    /// message; a smaller buffer can return `MessageTooLarge` without consuming it.
    pub fn try_receive(&self, bytes: &mut [u8]) -> Result<usize> {
        self.channel.as_byte_channel().try_receive(bytes)
    }
}

/// Reactor-owned endpoint with one pending chunk in each direction. The caller
/// must revoke `Console::set_open` when this endpoint closes or becomes invalid.
pub struct Port {
    channel: OwnedHandle<ByteChannelObject>,
    buffers: Buffers,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Poll {
    /// Service guest queues and poll again before sleeping when bytes moved.
    pub progress: bool,
    /// The host closed its endpoint. Drop this Port and revoke host-open state.
    pub closed: bool,
}

impl Port {
    pub fn as_handle_ref(&self) -> HandleRef<'_, ByteChannelObject> {
        self.channel.as_handle_ref()
    }

    /// Register exactly these signals after polling. On `PEER_CLOSED` the reactor
    /// must discard the session immediately, including when guest backpressure
    /// has disabled READABLE interest. Never subscribe to idle WRITABLE signals.
    pub fn signals(&self) -> ObjectSignals<ByteChannelObject> {
        let mut signals = ObjectSignals::<ByteChannelObject>::PEER_CLOSED;
        if self.buffers.input_start == self.buffers.input_end {
            signals = signals.union(ObjectSignals::<ByteChannelObject>::READABLE);
        }
        if self.buffers.output_end != 0 {
            signals = signals.union(ObjectSignals::<ByteChannelObject>::WRITABLE);
        }
        signals
    }

    /// Never blocks: at most one channel message and one guest chunk move in
    /// each direction. Guest/host backpressure retains bytes for the next turn.
    pub fn poll(&mut self, console: &mut Console) -> Result<Poll> {
        self.buffers.poll(&self.channel.as_byte_channel(), console)
    }
}

trait Endpoint {
    fn try_send(&self, bytes: &[u8]) -> Result<()>;
    fn try_receive(&self, bytes: &mut [u8]) -> Result<usize>;
}

impl Endpoint for ByteChannel<'_> {
    fn try_send(&self, bytes: &[u8]) -> Result<()> {
        ByteChannel::try_send(self, bytes)
    }
    fn try_receive(&self, bytes: &mut [u8]) -> Result<usize> {
        ByteChannel::try_receive(self, bytes)
    }
}

trait Stream {
    fn send(&mut self, bytes: &[u8]) -> usize;
    fn receive(&mut self, bytes: &mut [u8]) -> usize;
}

impl Stream for Console {
    fn send(&mut self, bytes: &[u8]) -> usize {
        Console::send(self, bytes)
    }
    fn receive(&mut self, bytes: &mut [u8]) -> usize {
        Console::receive(self, bytes)
    }
}

struct Buffers {
    input: Box<[u8; MAX_BYTES]>,
    input_start: usize,
    input_end: usize,
    output: Box<[u8; MAX_BYTES]>,
    output_end: usize,
}

impl Buffers {
    fn new() -> Self {
        Self {
            input: Box::new([0; MAX_BYTES]),
            input_start: 0,
            input_end: 0,
            output: Box::new([0; MAX_BYTES]),
            output_end: 0,
        }
    }

    fn poll(&mut self, channel: &impl Endpoint, stream: &mut impl Stream) -> Result<Poll> {
        let mut result = Poll::default();
        if self.input_start == self.input_end {
            match channel.try_receive(&mut self.input[..]) {
                Ok(length) if (1..=MAX_BYTES).contains(&length) => {
                    self.input_start = 0;
                    self.input_end = length;
                    result.progress = true;
                }
                Ok(_) => return Err(Error::InvalidResponse),
                Err(Error::Status(Status::WOULD_BLOCK)) => (),
                Err(Error::Status(Status::PEER_CLOSED)) => {
                    return Ok(Poll {
                        closed: true,
                        ..result
                    });
                }
                Err(error) => return Err(error),
            }
        }
        if self.input_start != self.input_end {
            let length = stream.send(&self.input[self.input_start..self.input_end]);
            if length > self.input_end - self.input_start {
                return Err(Error::InvalidResponse);
            }
            self.input_start += length;
            result.progress |= length != 0;
        }
        if self.output_end == 0 {
            self.output_end = stream.receive(&mut self.output[..]);
            if self.output_end > MAX_BYTES {
                return Err(Error::InvalidResponse);
            }
            result.progress |= self.output_end != 0;
        }
        if self.output_end != 0 {
            match channel.try_send(&self.output[..self.output_end]) {
                Ok(()) => {
                    self.output_end = 0;
                    result.progress = true;
                }
                Err(Error::Status(Status::WOULD_BLOCK)) => (),
                Err(Error::Status(Status::PEER_CLOSED)) => result.closed = true,
                Err(error) => return Err(error),
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
#[path = "../../tests/console_port.rs"]
mod tests;

impl crate::agent::Transport for HostPort {
    fn send(&self, bytes: &[u8]) -> std::result::Result<bool, crate::agent::Failure> {
        match self.try_send(bytes) {
            Ok(()) => Ok(true),
            Err(Error::Status(Status::WOULD_BLOCK)) => Ok(false),
            Err(_) => Err(crate::agent::Failure::Transport),
        }
    }
    fn receive(
        &self,
        bytes: &mut [u8],
    ) -> std::result::Result<Option<usize>, crate::agent::Failure> {
        match self.try_receive(bytes) {
            Ok(length) => Ok(Some(length)),
            Err(Error::Status(Status::WOULD_BLOCK)) => Ok(None),
            Err(_) => Err(crate::agent::Failure::Transport),
        }
    }
}
