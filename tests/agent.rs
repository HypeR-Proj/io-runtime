// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

#[derive(Default)]
struct Stream {
    sent: RefCell<Vec<Vec<u8>>>,
    received: RefCell<VecDeque<Vec<u8>>>,
    blocked: Cell<bool>,
}
impl Transport for Stream {
    fn send(&self, bytes: &[u8]) -> Result<bool, Failure> {
        if self.blocked.get() {
            return Ok(false);
        }
        self.sent.borrow_mut().push(bytes.to_vec());
        Ok(true)
    }
    fn receive(&self, bytes: &mut [u8]) -> Result<Option<usize>, Failure> {
        let Some(chunk) = self.received.borrow_mut().pop_front() else {
            return Ok(None);
        };
        bytes[..chunk.len()].copy_from_slice(&chunk);
        Ok(Some(chunk.len()))
    }
}
fn round(client: &mut Client, stream: &Stream, now: u64, value: Value) -> Value {
    client.poll(stream, now).unwrap();
    let bytes = stream.sent.borrow_mut().pop().unwrap();
    let sync = bytes[0] == 0xff;
    let request: Value = serde_json::from_slice(if sync { &bytes[1..] } else { &bytes }).unwrap();
    let value = if sync {
        request["arguments"]["id"].clone()
    } else {
        value
    };
    let mut response = serde_json::to_vec(&json!({"return":value, "id":request["id"]})).unwrap();
    if sync {
        response.insert(0, 0xff);
    }
    response.push(b'\n');
    // Exercise record framing independently of channel message boundaries.
    stream
        .received
        .borrow_mut()
        .extend(response.chunks(7).map(<[u8]>::to_vec));
    while !stream.received.borrow().is_empty() {
        client.poll(stream, now).unwrap();
    }
    request
}
fn cpu(user: u64, idle: u64) -> Value {
    json!([{"type":"linux","cpu":0,"user":user,"nice":0,"system":0,"idle":idle,
        "guest":user,"guestnice":0}])
}
fn memory() -> Value {
    json!({"total-bytes":131072,"available-bytes":65536,"free-bytes":32768,
        "buffers-bytes":4096,"cached-bytes":8192,"reclaimable-slab-bytes":4096})
}

#[test]
fn samples_are_bounded_fresh_and_use_counter_deltas() {
    let stream = Stream::default();
    let mut client = Client::new(0, 40, 256000).unwrap();
    assert_eq!(client.metrics(0).state, AgentState::Sampling);
    round(&mut client, &stream, 0, Value::Null);
    round(&mut client, &stream, 10, cpu(100, 900));
    round(&mut client, &stream, 20, memory());
    assert_eq!(client.metrics(20).state, AgentState::Sampling);
    assert!(client.metrics(20).memory.is_some());
    round(&mut client, &stream, 5020, cpu(600, 1400));
    round(&mut client, &stream, 5030, memory());
    let metrics = client.metrics(5100);
    assert_eq!(metrics.state, AgentState::Ready);
    assert_eq!(metrics.cpu.unwrap().busy_basis_points, 5000);
    assert_eq!(metrics.cpu.unwrap().interval_ms, 5010);
    assert_eq!(metrics.sample_age_ms, Some(70));
    assert_eq!(
        client.metrics(5030 + MAX_AGE_MS + 1).state,
        AgentState::Stale
    );
    assert!(client.metrics(5030 + MAX_AGE_MS + 1).memory.is_none());
}

#[test]
fn cpu_membership_and_counter_reset_need_a_new_baseline() {
    let previous = stats::cpus(cpu(100, 900)).unwrap();
    assert!(stats::usage(&previous, &stats::cpus(cpu(99, 1000)).unwrap(), 1000).is_none());
    assert!(stats::usage(&previous, &previous, 1000).is_none());
    let mut duplicate = cpu(200, 1000);
    let first = duplicate[0].clone();
    duplicate.as_array_mut().unwrap().push(first);
    assert!(stats::cpus(duplicate).is_none());
    let two = stats::cpus(json!([
        {"type":"linux","cpu":1,"user":200,"nice":0,"system":0,"idle":1800},
        {"type":"linux","cpu":0,"user":200,"nice":0,"system":0,"idle":1800}
    ]))
    .unwrap();
    assert!(stats::usage(&previous, &two, 1000).is_none());
    let later = stats::cpus(json!([
        {"type":"linux","cpu":0,"user":300,"nice":0,"system":0,"idle":1800},
        {"type":"linux","cpu":1,"user":200,"nice":0,"system":0,"idle":1900}
    ]))
    .unwrap();
    assert_eq!(
        stats::usage(&two, &later, 1000).unwrap().busy_basis_points,
        5000
    );
}

#[test]
fn framing_requires_sentinel_and_rejects_unbounded_or_invalid_records() {
    let mut decoder = framing::Decoder::new();
    for byte in b"stale{\n" {
        assert!(decoder.push(*byte).unwrap().is_none());
    }
    decoder.push(0xff).unwrap();
    for byte in b"{broken" {
        decoder.push(*byte).unwrap();
    }
    decoder.push(0xff).unwrap();
    for byte in b"{\"return\":1}\r" {
        decoder.push(*byte).unwrap();
    }
    assert_eq!(decoder.push(b'\n').unwrap(), Some(json!({"return":1})));
    for _ in 0..framing::MAX_REPLY {
        decoder.push(b'x').unwrap();
    }
    assert!(decoder.push(b'x').is_err());
    decoder.push(0xff).unwrap();
    decoder.push(b'x').unwrap();
    assert!(decoder.push(b'\n').is_err());
}

#[test]
fn backpressure_does_not_restart_deadline_or_duplicate_requests() {
    let stream = Stream::default();
    stream.blocked.set(true);
    let mut client = Client::new(0, 0, 256000).unwrap();
    assert!(!client.poll(&stream, 100).unwrap());
    assert_eq!(client.deadline(), TIMEOUT_MS);
    stream.blocked.set(false);
    assert!(client.poll(&stream, 200).unwrap());
    assert!(!client.poll(&stream, 300).unwrap());
    assert_eq!(stream.sent.borrow().len(), 1);
    assert_eq!(client.poll(&stream, TIMEOUT_MS), Err(Failure::Timeout));
}

#[test]
fn stale_ids_cannot_complete_a_new_sync_and_rpc_errors_are_explicit() {
    let stream = Stream::default();
    let mut client = Client::new(0, 100, 256000).unwrap();
    client.poll(&stream, 0).unwrap();
    stream
        .received
        .borrow_mut()
        .push_back(b"\xff{\"return\":1,\"id\":1}\n".to_vec());
    client.poll(&stream, 1).unwrap();
    assert_eq!(client.sequence(), 101);
    assert_eq!(client.metrics(1).state, AgentState::Sampling);
    stream
        .received
        .borrow_mut()
        .push_back(b"{\"id\":101,\"error\":{\"class\":\"CommandNotFound\"}}\n".to_vec());
    assert_eq!(client.poll(&stream, 2), Err(Failure::Unsupported));
}

#[test]
fn memory_cannot_exceed_owned_ram_or_report_negative_available() {
    assert!(stats::memory(memory(), 100).is_none());
    let mut bad = memory();
    bad["available-bytes"] = json!(-1);
    assert!(stats::memory(bad, 256000).is_none());
    let mut bad = memory();
    bad["available-bytes"] = json!(999999);
    assert!(stats::memory(bad, 256000).is_none());
}

#[test]
fn busy_excludes_iowait_and_steal_without_double_counting_guest_time() {
    let before = stats::cpus(cpu(0, 0)).unwrap();
    let after = stats::cpus(json!([{
        "type":"linux", "cpu":0, "user":100, "nice":0, "system":0,
        "idle":100, "iowait":100, "steal":100, "guest":100
    }]))
    .unwrap();
    let usage = stats::usage(&before, &after, 400).unwrap();
    assert_eq!(usage.busy_basis_points, 2500);
    assert_eq!(usage.iowait_basis_points, 2500);
}

#[test]
fn unsynchronized_traffic_has_a_per_turn_work_limit() {
    let stream = Stream::default();
    stream
        .received
        .borrow_mut()
        .extend((0..20).map(|_| vec![b'x'; 4096]));
    let mut client = Client::new(0, 0, 256000).unwrap();
    assert!(client.poll(&stream, 1).unwrap());
    assert_eq!(stream.received.borrow().len(), 16);
    assert_eq!(client.poll(&stream, TIMEOUT_MS), Err(Failure::Timeout));
}
