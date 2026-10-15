// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Wire contracts with Native clients and the Linux I/O appliance.
//! Device emulation and frontend MMIO state belong to the VM runtime.

pub mod block;
pub mod control;

pub const NETWORK_QUEUES: usize = 2;
pub const VERSION_1: u64 = 1 << 32;
pub const REQUEST_QUEUES: usize = 4;
pub const QUEUES: usize = 2 + REQUEST_QUEUES;
pub const QUEUE_MAX: u32 = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum DeviceKind {
    Scsi = 0,
    Network = 1,
}

impl DeviceKind {
    pub const fn queue_count(self) -> usize {
        match self {
            Self::Scsi => QUEUES,
            Self::Network => NETWORK_QUEUES,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Queue {
    pub size: u32,
    pub descriptor: u64,
    pub available: u64,
    pub used: u64,
    pub ready: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendOperation {
    Activate {
        features: u64,
        queues: [Queue; QUEUES],
    },
    Reset,
    /// Stop the vhost endpoint and drain in-flight I/O before acknowledging a
    /// queue stop. This backend cannot independently retire one SCSI queue;
    /// successful quiescence requests a whole-device reset from the guest.
    StopQueue {
        queue: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkConfiguration {
    pub mac: [u8; 6],
    pub mtu: u16,
}

impl NetworkConfiguration {
    pub fn valid(self) -> bool {
        // Deployment assigns a stable, locally administered unicast address.
        self.mac[0] & 3 == 2 && self.mtu == 1500
    }
}
