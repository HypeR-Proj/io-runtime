// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Optional telemetry consumer; failure never changes I/O VM lifecycle.

use hyper_io_runtime_system::agent::{Client, Failure, SAMPLE_MS};
use hyper_io_runtime_system::devices::{port::HostPort, worker::Worker};
use hyper_os::handle::ByteChannelObject;
use hyper_os::wait::{ObjectSignals, WaitItem};
use hyper_service::io::metrics::{AgentState, GuestMetrics};

pub(super) struct Agent {
    connection: Option<(HostPort, Client)>,
    retry_at: u64,
    sequence: u64,
    state: AgentState,
    ram_bytes: u64,
}

impl Agent {
    pub fn new(ram_bytes: u64) -> Self {
        Self {
            connection: None,
            retry_at: 0,
            sequence: 0,
            state: AgentState::Unavailable,
            ram_bytes,
        }
    }

    pub fn metrics(&self, now: u64) -> GuestMetrics {
        self.connection.as_ref().map_or(
            GuestMetrics {
                state: self.state,
                ..GuestMetrics::default()
            },
            |(_, client)| client.metrics(now),
        )
    }

    pub fn poll(&mut self, worker: &Worker, now: u64) -> bool {
        if self.connection.is_none() && now >= self.retry_at {
            self.retry_at = now.saturating_add(SAMPLE_MS);
            if let Ok(port) = worker.connect_console()
                && let Ok(client) = Client::new(now, self.sequence, self.ram_bytes)
            {
                self.connection = Some((port, client));
            }
        }
        let Some((port, client)) = &mut self.connection else {
            return false;
        };
        match client.poll(port, now) {
            Ok(progress) => progress,
            Err(error) => {
                self.sequence = client.sequence();
                self.connection = None;
                self.state = if error == Failure::Unsupported {
                    AgentState::Unsupported
                } else {
                    AgentState::Unavailable
                };
                self.retry_at = now.saturating_add(if error == Failure::Unsupported {
                    30000
                } else {
                    SAMPLE_MS
                });
                true
            }
        }
    }

    pub fn wait_item(&self) -> Option<WaitItem<'_>> {
        self.connection.as_ref().map(|(port, client)| {
            let mut signals = ObjectSignals::<ByteChannelObject>::READABLE
                .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED);
            if client.wants_write() {
                signals = signals.union(ObjectSignals::<ByteChannelObject>::WRITABLE);
            }
            WaitItem::new(port.as_handle_ref(), signals)
        })
    }

    pub fn deadline(&self) -> u64 {
        self.connection
            .as_ref()
            .map_or(self.retry_at, |(_, client)| client.deadline())
            .saturating_mul(1_000_000)
    }
}
