// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Board-authored disk and network policy shared by Native and Linux supervisors.

use hyper_vm_image::guest_fdt::io::DmaRange;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Client {
    pub id: u32,
    pub volume: Option<String>,
    pub network: Option<String>,
    pub mac: [u8; 6],
}

impl Client {
    /// The same media or network identity cannot belong to two client slots.
    pub(crate) fn conflicts_with(&self, other: &Self) -> bool {
        self.id == other.id
            || (self.volume.is_some() && other.volume == self.volume)
            || (self.network.is_some() && other.mac == self.mac)
    }

    pub fn connection(&self) -> hyper_service::io::Connection<'_> {
        hyper_service::io::Connection {
            client: self.id,
            volume: self.volume.as_deref(),
            network: self.network.as_deref(),
            mac: self.mac,
        }
    }

    pub fn devices(&self) -> u32 {
        self.connection().devices()
    }
}

pub fn parse(bytes: &str) -> Result<Vec<Client>, &'static str> {
    let mut lines = bytes.lines();
    if bytes.len() > 8192 || lines.next() != Some("hyper.clients.v3") {
        return Err("invalid I/O client table header or size");
    }
    let mut clients = Vec::new();
    let mut management = false;
    for line in lines {
        let fields: Vec<_> = line.split(' ').collect();
        let [id, volume, network, mac] = fields.as_slice() else {
            return Err("invalid I/O client entry");
        };
        let id: u32 = id.parse().map_err(|_| "invalid I/O client identity")?;
        if id == 0 {
            if management || (*volume, *network, *mac) != ("-", "-", "-") {
                return Err("invalid management client");
            }
            // Native's network identity is reserved, with no live endpoint.
            management = true;
            continue;
        }
        let client = Client {
            id,
            volume: (*volume != "-").then(|| (*volume).into()),
            network: (*network != "-").then(|| (*network).into()),
            mac: if *mac == "-" {
                [0; 6]
            } else {
                hyper_service::io::parse_mac(mac).ok_or("invalid client MAC")?
            },
        };
        if client.connection().encode().is_none()
            || clients.iter().any(|other| client.conflicts_with(other))
        {
            return Err("invalid or duplicate I/O client");
        }
        clients.push(client);
    }
    if !management {
        return Err("management client is required");
    }
    Ok(clients)
}

/// The Native connection owner supplies identity and notification epoch; a
/// runtime can request device operations but cannot choose a new DMA session.
pub fn authorize_request(
    request: crate::protocol::control::Request,
    identity: u64,
    epoch: u32,
) -> bool {
    use crate::protocol::control::Command;
    request.binding == identity
        && request.epoch == epoch
        && matches!(
            request.command,
            Command::Hello | Command::Device(_) | Command::NetworkHello | Command::NetworkDevice(_)
        )
}

pub fn dynamic_dma_ranges(excluded: &[DmaRange]) -> Result<Vec<DmaRange>, String> {
    let mut excluded = excluded.to_vec();
    excluded.sort_by_key(|range| range.dma_base);
    let mut cursor = 0;
    let mut ranges = Vec::new();
    for range in excluded {
        let end = range
            .dma_base
            .checked_add(range.size)
            .ok_or("DMA range overflow")?;
        if range.dma_base < cursor || end > hyper_os::vm::DYNAMIC_PHYSICAL_LIMIT {
            return Err("invalid static DMA ranges".into());
        }
        if range.dma_base > cursor {
            ranges.push(DmaRange {
                dma_base: cursor,
                cpu_base: hyper_os::vm::DYNAMIC_ALIAS_OFFSET + cursor,
                size: range.dma_base - cursor,
            });
        }
        cursor = end;
    }
    if cursor < hyper_os::vm::DYNAMIC_PHYSICAL_LIMIT {
        ranges.push(DmaRange {
            dma_base: cursor,
            cpu_base: hyper_os::vm::DYNAMIC_ALIAS_OFFSET + cursor,
            size: hyper_os::vm::DYNAMIC_PHYSICAL_LIMIT - cursor,
        });
    }
    Ok(ranges)
}

#[cfg(test)]
#[path = "../tests/clients.rs"]
mod tests;
