// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

/// RAM budget for the resident, multi-client I/O appliance.
pub const RAM_BYTES: u64 = 64 * 1024 * 1024;

#[cfg(target_os = "hyper")]
pub mod install;
