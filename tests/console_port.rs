// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

#[derive(Default)]
struct Channel {
    input: RefCell<VecDeque<Vec<u8>>>,
    output: RefCell<Vec<Vec<u8>>>,
    writable: Cell<bool>,
    closed: Cell<bool>,
    reads: Cell<usize>,
}

impl Endpoint for Channel {
    fn try_send(&self, bytes: &[u8]) -> Result<()> {
        if self.closed.get() {
            return Err(Error::Status(Status::PEER_CLOSED));
        }
        if !self.writable.get() {
            return Err(Error::Status(Status::WOULD_BLOCK));
        }
        self.output.borrow_mut().push(bytes.into());
        Ok(())
    }
    fn try_receive(&self, bytes: &mut [u8]) -> Result<usize> {
        self.reads.set(self.reads.get() + 1);
        if self.closed.get() {
            return Err(Error::Status(Status::PEER_CLOSED));
        }
        let mut input = self.input.borrow_mut();
        let message = input.front().ok_or(Error::Status(Status::WOULD_BLOCK))?;
        if message.len() > bytes.len() {
            return Err(Error::MessageTooLarge {
                bytes: message.len() as u64,
                handles: 0,
            });
        }
        let length = message.len();
        bytes[..length].copy_from_slice(message);
        input.pop_front();
        Ok(length)
    }
}

#[derive(Default)]
struct Guest {
    input: Vec<u8>,
    output: VecDeque<u8>,
    writable: usize,
}

impl Stream for Guest {
    fn send(&mut self, bytes: &[u8]) -> usize {
        let length = bytes.len().min(self.writable);
        self.input.extend(&bytes[..length]);
        self.writable -= length;
        length
    }
    fn receive(&mut self, bytes: &mut [u8]) -> usize {
        let length = bytes.len().min(self.output.len());
        for byte in &mut bytes[..length] {
            if let Some(value) = self.output.pop_front() {
                *byte = value;
            }
        }
        length
    }
}

#[test]
fn guest_backpressure_preserves_partial_chunk_and_bounds_channel_reads() -> Result<()> {
    let channel = Channel::default();
    channel
        .input
        .borrow_mut()
        .extend([b"abcde".to_vec(), b"fgh".to_vec()]);
    let mut guest = Guest {
        writable: 2,
        ..Guest::default()
    };
    let mut buffers = Buffers::new();
    assert!(buffers.poll(&channel, &mut guest)?.progress);
    assert_eq!(guest.input, b"ab");
    assert_eq!(channel.reads.get(), 1);
    assert_eq!(buffers.poll(&channel, &mut guest)?, Poll::default());
    assert_eq!(channel.reads.get(), 1);
    guest.writable = 6;
    assert!(buffers.poll(&channel, &mut guest)?.progress);
    assert_eq!(guest.input, b"abcde");
    assert_eq!(channel.reads.get(), 1);
    assert!(buffers.poll(&channel, &mut guest)?.progress);
    assert_eq!(guest.input, b"abcdefgh");
    Ok(())
}

#[test]
fn host_backpressure_retains_whole_chunk_without_redraining_guest() -> Result<()> {
    let channel = Channel::default();
    let mut guest = Guest::default();
    guest
        .output
        .extend((0..MAX_BYTES + 17).map(|index| index as u8));
    let expected: Vec<_> = guest.output.iter().copied().collect();
    let mut buffers = Buffers::new();
    assert!(buffers.poll(&channel, &mut guest)?.progress);
    assert_eq!(guest.output.len(), 17);
    assert_eq!(buffers.poll(&channel, &mut guest)?, Poll::default());
    assert_eq!(guest.output.len(), 17);
    channel.writable.set(true);
    assert!(buffers.poll(&channel, &mut guest)?.progress);
    assert!(buffers.poll(&channel, &mut guest)?.progress);
    assert_eq!(channel.output.borrow().concat(), expected);
    assert_eq!(buffers.poll(&channel, &mut guest)?, Poll::default());
    Ok(())
}

#[test]
fn simultaneous_backpressure_does_not_block_the_other_direction() -> Result<()> {
    let channel = Channel::default();
    channel.input.borrow_mut().push_back(b"request".to_vec());
    let mut guest = Guest::default();
    guest.output.extend(b"reply");
    let mut buffers = Buffers::new();
    assert!(buffers.poll(&channel, &mut guest)?.progress);
    assert_eq!(buffers.input_end, 7);
    assert_eq!(buffers.output_end, 5);
    channel.writable.set(true);
    assert!(buffers.poll(&channel, &mut guest)?.progress);
    assert_eq!(channel.output.borrow().concat(), b"reply");
    assert!(guest.input.is_empty());
    guest.writable = MAX_BYTES;
    assert!(buffers.poll(&channel, &mut guest)?.progress);
    assert_eq!(guest.input, b"request");
    Ok(())
}

#[test]
fn peer_close_is_reported_without_delivery_into_closed_session() -> Result<()> {
    let channel = Channel::default();
    channel.closed.set(true);
    let mut guest = Guest::default();
    guest.output.extend(b"discard");
    let mut buffers = Buffers::new();
    assert_eq!(
        buffers.poll(&channel, &mut guest)?,
        Poll {
            progress: false,
            closed: true
        }
    );
    assert_eq!(guest.output.len(), 7);
    Ok(())
}

#[test]
fn malformed_empty_or_oversized_messages_fail_the_session() {
    for message in [Vec::new(), vec![0; MAX_BYTES + 1]] {
        let channel = Channel::default();
        channel.input.borrow_mut().push_back(message);
        let mut guest = Guest::default();
        let mut buffers = Buffers::new();
        assert!(buffers.poll(&channel, &mut guest).is_err());
        assert!(guest.input.is_empty());
    }
}
