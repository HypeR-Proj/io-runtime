// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::panic))]

pub mod admission_policy;
pub mod agent;
pub mod broker_exchange;
pub mod clients;
pub mod deadline;
#[cfg(target_os = "hyper")]
pub mod devices;
pub mod guest;
pub mod guest_log;
pub mod handoff;
pub mod protocol;
pub mod sdhci;

#[cfg(test)]
#[path = "devices/port.rs"]
mod console_port;
