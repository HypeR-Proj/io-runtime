// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Adopted live machine; system never reloads its boot image.

use hyper_os::handle::{OwnedHandle, VirtualCpuObject, VirtualMachineObject};
use hyper_os::virtual_serial::Output;
use std::sync::Arc;

pub use super::RAM_BYTES;
pub const PHYSICAL_MMIO: u64 = 0x0b00_0000;
pub struct InstalledGuest {
    pub name: String,
    pub image: String,
    pub machine: Arc<OwnedHandle<VirtualMachineObject>>,
    pub cpus: Vec<Arc<OwnedHandle<VirtualCpuObject>>>,
    pub output: Output,
}
impl InstalledGuest {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn image(&self) -> &str {
        &self.image
    }
}
