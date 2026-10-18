// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Formal-environment reporting; bootstrap needs no fault consumer or policy.

use super::adopt::Adopted;
use hyper_os::{
    device,
    handle::{HandleRef, PhysicalDeviceObject},
    wait::{ObjectSignals, WaitItem},
};
use std::io::Write;

#[derive(Default)]
struct Watch {
    reported: bool,
    probe: bool,
    index: Option<usize>,
}

pub(super) struct Monitor {
    watches: [Watch; 2],
    next_probe_ms: u64,
}

impl Monitor {
    pub(super) fn new() -> Self {
        Self {
            watches: core::array::from_fn(|_| Watch {
                probe: true,
                ..Watch::default()
            }),
            next_probe_ms: 0,
        }
    }

    pub(super) fn poll(&mut self, owner: &Adopted, now_ms: u64) {
        // Busy broker traffic or a permanently readable lower-index wait item
        // must not starve fault inspection. Idle waits still wake on DMA_FAULT;
        // this bounded fallback adds no periodic timer or idle wakeups.
        if now_ms >= self.next_probe_ms {
            for watch in &mut self.watches {
                watch.probe = true;
            }
            self.next_probe_ms = now_ms.saturating_add(1000);
        }
        for (watch, (name, device)) in self.watches.iter_mut().zip(devices(owner)) {
            if !watch.probe || watch.reported {
                continue;
            }
            watch.probe = false;
            let Some(device) = device else { continue };
            match device::dma_fault(device) {
                Ok(None) => (),
                Ok(Some(fault)) => {
                    // stderr is connected to this component's journal stream.
                    // Reporting must not turn a sink failure into VM teardown.
                    let address = fault
                        .address
                        .map_or_else(|| "unavailable".into(), |address| format!("{address:#x}"));
                    let access = match fault.read {
                        Some(true) => "read",
                        Some(false) => "write",
                        None => "unavailable",
                    };
                    let _ = writeln!(
                        std::io::stderr().lock(),
                        "HypeR io-runtime: DMA fault: device={name} source=SMMUv3 scope={:?} sid={} code={:#x} address={address} access={access} occurred_at_ns={}; action=report-only",
                        fault.scope,
                        fault.stream_id,
                        fault.code,
                        fault.timestamp_ns
                    );
                    watch.reported = true;
                }
                Err(error) => {
                    let _ = writeln!(
                        std::io::stderr().lock(),
                        "HypeR io-runtime: DMA fault observation unavailable: device={name}: {error:?}"
                    );
                    watch.reported = true;
                }
            }
        }
    }

    pub(super) fn append_waits<'a>(&mut self, owner: &'a Adopted, items: &mut Vec<WaitItem<'a>>) {
        for (watch, (_, device)) in self.watches.iter_mut().zip(devices(owner)) {
            watch.index = None;
            if !watch.reported
                && let Some(device) = device
            {
                watch.index = Some(items.len());
                items.push(WaitItem::new(
                    device,
                    ObjectSignals::<PhysicalDeviceObject>::DMA_FAULT,
                ));
            }
        }
    }

    pub(super) fn wake(&mut self, index: usize) {
        for watch in &mut self.watches {
            if watch.index == Some(index) {
                watch.probe = true;
            }
        }
    }
}

fn devices(owner: &Adopted) -> [(&'static str, Option<HandleRef<'_, PhysicalDeviceObject>>); 2] {
    [
        ("storage", Some(owner.physical.as_handle_ref())),
        (
            "network",
            owner.network.as_ref().map(|device| device.as_handle_ref()),
        ),
    ]
}
