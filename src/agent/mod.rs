// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Read-only QGA sampling. A stalled agent cannot block storage or VM control.

mod framing;
mod stats;

use hyper_service::io::metrics::{AgentState, GuestMetrics, MemoryUsage};
use serde_json::{Value, json};

pub const SAMPLE_MS: u64 = 5000;
const TIMEOUT_MS: u64 = 2000;
const MAX_AGE_MS: u64 = 15000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Transport,
    Protocol,
    Timeout,
    Unsupported,
}

pub trait Transport {
    fn send(&self, bytes: &[u8]) -> Result<bool, Failure>;
    fn receive(&self, bytes: &mut [u8]) -> Result<Option<usize>, Failure>;
}

#[derive(Clone, Copy)]
enum Command {
    Sync,
    Cpu,
    Memory,
}

struct Pending {
    command: Command,
    id: u64,
    bytes: Vec<u8>,
    sent: bool,
    deadline: u64,
}

/// One connection, one outstanding request, and one retained sample. Native
/// monotonic time supplies deadlines/freshness; guest time is only counter data.
pub struct Client {
    decoder: framing::Decoder,
    pending: Option<Pending>,
    sequence: u64,
    next_sample: u64,
    previous: Option<(u64, Vec<stats::Cpu>)>,
    current: Option<(u64, Vec<stats::Cpu>)>,
    sample: Option<(u64, GuestMetrics)>,
    ram_bytes: u64,
}

impl Client {
    pub fn new(now: u64, sequence: u64, ram_bytes: u64) -> Result<Self, Failure> {
        let mut client = Self {
            decoder: framing::Decoder::new(),
            pending: None,
            sequence,
            next_sample: now,
            previous: None,
            current: None,
            sample: None,
            ram_bytes,
        };
        client.request(Command::Sync, now)?;
        Ok(client)
    }

    pub fn metrics(&self, now: u64) -> GuestMetrics {
        let Some((at, mut metrics)) = self.sample else {
            return GuestMetrics {
                state: AgentState::Sampling,
                ..GuestMetrics::default()
            };
        };
        let age = now.saturating_sub(at);
        if age > MAX_AGE_MS {
            return GuestMetrics {
                state: AgentState::Stale,
                sample_age_ms: Some(age),
                ..GuestMetrics::default()
            };
        }
        metrics.sample_age_ms = Some(age);
        metrics
    }

    pub fn deadline(&self) -> u64 {
        self.pending
            .as_ref()
            .map_or(self.next_sample, |pending| pending.deadline)
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn wants_write(&self) -> bool {
        self.pending.as_ref().is_some_and(|pending| !pending.sent)
    }

    /// Service at most four channel chunks per reactor turn, including invalid
    /// traffic. The enclosing event loop must get a chance to service storage.
    pub fn poll(&mut self, transport: &impl Transport, now: u64) -> Result<bool, Failure> {
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| now >= pending.deadline)
        {
            return Err(Failure::Timeout);
        }
        if self.pending.is_none() && now >= self.next_sample {
            self.request(Command::Cpu, now)?;
        }
        let mut progress = false;
        if let Some(pending) = &mut self.pending
            && !pending.sent
        {
            pending.sent = transport.send(&pending.bytes)?;
            progress |= pending.sent;
        }
        let mut bytes = [0; 4096];
        for _ in 0..4 {
            let Some(count) = transport.receive(&mut bytes)? else {
                break;
            };
            if count == 0 || count > bytes.len() {
                return Err(Failure::Protocol);
            }
            progress = true;
            for byte in &bytes[..count] {
                if let Some(reply) = self.decoder.push(*byte).map_err(|()| Failure::Protocol)? {
                    self.response(reply, now)?;
                }
            }
        }
        Ok(progress)
    }

    fn request(&mut self, command: Command, now: u64) -> Result<(), Failure> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .filter(|id| *id <= i64::MAX as u64)
            .ok_or(Failure::Protocol)?;
        let id = self.sequence;
        let request = match command {
            Command::Sync => {
                json!({"execute":"guest-sync-delimited", "arguments":{"id":id}, "id":id})
            }
            Command::Cpu => json!({"execute":"guest-get-cpustats", "id":id}),
            Command::Memory => json!({"execute":"guest-hyper-get-memory-stats", "id":id}),
        };
        let mut bytes = serde_json::to_vec(&request).map_err(|_| Failure::Protocol)?;
        if matches!(command, Command::Sync) {
            bytes.insert(0, 0xff);
        }
        bytes.push(b'\n');
        self.pending = Some(Pending {
            command,
            id,
            bytes,
            sent: false,
            deadline: now.saturating_add(TIMEOUT_MS),
        });
        Ok(())
    }

    fn response(&mut self, reply: Value, now: u64) -> Result<(), Failure> {
        let pending = self.pending.as_ref().ok_or(Failure::Protocol)?;
        // Earlier sessions may leave replies in the stream. Only the current
        // request ID can complete a transaction, including synchronization.
        if reply.get("id").and_then(Value::as_u64) != Some(pending.id) {
            return Ok(());
        }
        if !pending.sent {
            return Err(Failure::Protocol);
        }
        if let Some(error) = reply.get("error") {
            return Err(
                if matches!(
                    error.get("class").and_then(Value::as_str),
                    Some("CommandNotFound" | "CommandDisabled")
                ) {
                    Failure::Unsupported
                } else {
                    Failure::Protocol
                },
            );
        }
        let value = reply.get("return").ok_or(Failure::Protocol)?.clone();
        let command = pending.command;
        let id = pending.id;
        self.pending = None;
        match command {
            Command::Sync => {
                if value.as_u64() != Some(id) {
                    return Err(Failure::Protocol);
                }
                self.request(Command::Cpu, now)?;
            }
            Command::Cpu => {
                self.current = Some((now, stats::cpus(value).ok_or(Failure::Protocol)?));
                self.request(Command::Memory, now)?;
            }
            Command::Memory => {
                let memory = stats::memory(value, self.ram_bytes).ok_or(Failure::Protocol)?;
                self.publish(memory, now)?;
            }
        }
        Ok(())
    }

    fn publish(&mut self, memory: MemoryUsage, now: u64) -> Result<(), Failure> {
        let (at, cpus) = self.current.take().ok_or(Failure::Protocol)?;
        let cpu = self.previous.as_ref().and_then(|(before, previous)| {
            stats::usage(previous, &cpus, at.saturating_sub(*before))
        });
        self.previous = Some((at, cpus));
        self.sample = Some((
            now,
            GuestMetrics {
                state: if cpu.is_some() {
                    AgentState::Ready
                } else {
                    AgentState::Sampling
                },
                sample_age_ms: Some(0),
                cpu,
                memory: Some(memory),
            },
        ));
        self.next_sample = now.saturating_add(SAMPLE_MS);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/agent.rs"]
mod tests;
