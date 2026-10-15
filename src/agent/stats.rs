// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Validate Linux counters before publishing guest-reported usage.

use hyper_service::io::metrics::{CpuUsage, MemoryUsage};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Cpu {
    #[serde(rename = "type")]
    kind: String,
    cpu: u32,
    user: u64,
    nice: u64,
    system: u64,
    idle: u64,
    #[serde(default)]
    iowait: u64,
    #[serde(default)]
    irq: u64,
    #[serde(default)]
    softirq: u64,
    #[serde(default)]
    steal: u64,
    // Linux user/nice already include guest/guestnice. Never add them twice.
}

pub fn cpus(value: serde_json::Value) -> Option<Vec<Cpu>> {
    let mut cpus: Vec<Cpu> = serde_json::from_value(value).ok()?;
    if cpus.is_empty() || cpus.len() > 8 {
        return None;
    }
    cpus.sort_by_key(|cpu| cpu.cpu);
    if cpus.iter().any(|cpu| cpu.kind != "linux" || cpu.cpu >= 8)
        || cpus.windows(2).any(|pair| pair[0].cpu == pair[1].cpu)
    {
        return None;
    }
    Some(cpus)
}

pub fn usage(previous: &[Cpu], current: &[Cpu], interval_ms: u64) -> Option<CpuUsage> {
    if previous.len() != current.len() || interval_ms == 0 {
        return None;
    }
    let mut busy = 0u128;
    let mut total = 0u128;
    let mut waiting = 0u128;
    for (before, after) in previous.iter().zip(current) {
        if before.cpu != after.cpu {
            return None;
        }
        let fields = |cpu: &Cpu| {
            [
                cpu.user,
                cpu.nice,
                cpu.system,
                cpu.irq,
                cpu.softirq,
                cpu.idle,
                cpu.iowait,
                cpu.steal,
            ]
        };
        for (index, (before, after)) in fields(before).into_iter().zip(fields(after)).enumerate() {
            let delta = u128::from(after.checked_sub(before)?);
            total += delta;
            if index < 5 {
                busy += delta;
            }
            if index == 6 {
                waiting += delta;
            }
        }
    }
    if total == 0 {
        return None;
    }
    Some(CpuUsage {
        busy_basis_points: u32::try_from(busy * 10000 / total).ok()?,
        iowait_basis_points: u32::try_from(waiting * 10000 / total).ok()?,
        interval_ms,
    })
}

pub fn memory(value: serde_json::Value, ram_bytes: u64) -> Option<MemoryUsage> {
    let memory: MemoryUsage = serde_json::from_value(value).ok()?;
    (memory.valid() && memory.total_bytes <= ram_bytes).then_some(memory)
}
