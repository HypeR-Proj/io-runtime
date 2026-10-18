// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Versioned, capability-backed transfer of an already running I/O appliance.
//! No process-local handle value or virtual address crosses this boundary.

use crate::clients::Client;
use serde::{Deserialize, Serialize};

pub const ENDPOINT: u32 = 0x8008_0010;
pub const DESCRIPTION: u32 = 0x8008_0012;
pub const READY: u32 = 0x8008_0020;
pub const DESCRIBE_MESSAGE: &[u8] = b"HYPER-IO-DESCRIBE/2";
pub const ADOPT_MESSAGE: &[u8] = b"HYPER-IO-ADOPT/2";
pub const READY_MESSAGE: &[u8] = b"HYPER-IO-READY/2";
pub const MAX_DESCRIPTION: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkerMode {
    None,
    Sdhci,
    VirtioTest,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Kind {
    Machine,
    Cpu,
    Serial,
    SerialBuffer,
    OwnMemory,
    HotplugInterrupt,
    StorageDevice,
    NetworkDevice,
    Broker,
    NativeMemory,
    NativeMailbox,
    BrokerMailbox,
}
impl Kind {
    pub fn required_rights(self) -> hyper_os::Rights {
        use hyper_os::Rights;
        let owner = Rights::TRANSFER;
        match self {
            Self::Machine => owner
                .union(Rights::WRITE)
                .union(Rights::WAIT)
                .union(Rights::INSPECT)
                .union(Rights::REQUEST_STOP),
            Self::Cpu => owner
                .union(Rights::WRITE)
                .union(Rights::WAIT)
                .union(Rights::INSPECT)
                .union(Rights::START),
            Self::Serial => owner.union(Rights::READ).union(Rights::WAIT),
            Self::OwnMemory => owner.union(Rights::READ).union(Rights::WRITE),
            Self::HotplugInterrupt => owner.union(Rights::WRITE),
            Self::SerialBuffer => owner.union(Rights::READ).union(Rights::MAP),
            Self::StorageDevice | Self::NetworkDevice => owner.union(Rights::INSPECT),
            Self::Broker => owner.union(Rights::READ).union(Rights::WAIT),
            Self::NativeMemory => owner.union(Rights::MAP),
            Self::NativeMailbox | Self::BrokerMailbox => owner
                .union(Rights::READ)
                .union(Rights::WRITE)
                .union(Rights::WAIT),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Slot {
    pub kind: Kind,
    pub rights: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Description {
    pub version: u32,
    pub name: String,
    pub image: String,
    pub worker: WorkerMode,
    pub cpus: usize,
    pub memory_base: u64,
    pub memory_size: u64,
    pub native: usize,
    pub network: bool,
    pub broker: bool,
    pub clients: Vec<Client>,
    pub slots: Vec<Slot>,
}
impl Description {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != 2
            || self.memory_base != 0x4000_0000
            || self.memory_size != crate::guest::RAM_BYTES
            || self.name.is_empty()
            || self.name.len() > 64
            || self.image.len() > 1024
            || !(1..=8).contains(&self.cpus)
            || self.native > hyper_os::vm::IO_MAX_CLIENTS
            || self.clients.len() >= hyper_os::vm::IO_MAX_CLIENTS
            || (!self.broker && !self.clients.is_empty())
        {
            return Err("invalid I/O handoff description");
        }
        let mut kinds = vec![Kind::Machine];
        kinds.extend(std::iter::repeat_n(Kind::Cpu, self.cpus));
        kinds.extend([
            Kind::Serial,
            Kind::SerialBuffer,
            Kind::OwnMemory,
            Kind::HotplugInterrupt,
            Kind::StorageDevice,
        ]);
        if self.network {
            kinds.push(Kind::NetworkDevice);
        }
        if self.broker {
            kinds.push(Kind::Broker);
        }
        kinds.extend(std::iter::repeat_n(Kind::NativeMemory, self.native));
        kinds.extend(std::iter::repeat_n(Kind::NativeMailbox, self.native.max(1)));
        kinds.extend(std::iter::repeat_n(Kind::BrokerMailbox, self.clients.len()));
        if kinds.len() > 64
            || kinds.len() != self.slots.len()
            || kinds
                .iter()
                .zip(&self.slots)
                .any(|(kind, slot)| *kind != slot.kind)
        {
            return Err("invalid I/O capability inventory");
        }
        if self.slots.iter().any(|slot| {
            hyper_os::Rights::from_bits(slot.rights)
                .is_none_or(|rights| !rights.contains(slot.kind.required_rights()))
        }) {
            return Err("insufficient I/O owner rights");
        }
        if self.worker != WorkerMode::None {
            let physical = self
                .slots
                .iter()
                .find(|slot| slot.kind == Kind::StorageDevice)
                .ok_or("missing userspace physical device")?;
            let required = hyper_os::Rights::READ
                .union(hyper_os::Rights::WRITE)
                .union(hyper_os::Rights::WAIT)
                .union(hyper_os::Rights::ACK_INTERRUPT)
                .union(hyper_os::Rights::DUPLICATE);
            if hyper_os::Rights::from_bits(physical.rights)
                .is_none_or(|rights| !rights.contains(required))
            {
                return Err("insufficient physical worker rights");
            }
        }
        for (index, client) in self.clients.iter().enumerate() {
            if client.id == 0
                || client.id as usize >= hyper_os::vm::IO_MAX_CLIENTS
                || client.connection().encode().is_none()
                || self.clients[..index]
                    .iter()
                    .any(|other| client.conflicts_with(other))
            {
                return Err("invalid I/O client policy");
            }
        }
        Ok(())
    }
}

/// Cursor and prefix state are captured after the last predecessor read.
pub fn encode_commit(
    cursor: u64,
    line_start: bool,
    pcie: &hyper_vm_support::pcie::RootPort,
) -> Vec<u8> {
    let mut bytes = ADOPT_MESSAGE.to_vec();
    bytes.extend(cursor.to_le_bytes());
    bytes.push(u8::from(line_start));
    bytes.extend(pcie.state());
    bytes
}
pub fn decode_commit(
    bytes: &[u8],
) -> Result<(u64, bool, hyper_vm_support::pcie::RootPort), &'static str> {
    let suffix = bytes
        .strip_prefix(ADOPT_MESSAGE)
        .ok_or("invalid I/O adoption record")?;
    if suffix.len() != 9 + hyper_vm_support::pcie::STATE_BYTES || suffix[8] > 1 {
        return Err("invalid I/O adoption length or log state");
    }
    let cursor = u64::from_le_bytes(suffix[..8].try_into().map_err(|_| "invalid I/O cursor")?);
    let pcie = hyper_vm_support::pcie::RootPort::from_state(&suffix[9..])
        .ok_or("invalid PCIe handoff state")?;
    if pcie.present() {
        return Err("bootstrap PCIe slot is not empty");
    }
    Ok((cursor, suffix[8] != 0, pcie))
}

#[cfg(test)]
#[path = "../tests/handoff.rs"]
mod tests;
