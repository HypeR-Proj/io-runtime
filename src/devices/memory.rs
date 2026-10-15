// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Device access to the appliance's own RAM, excluding all imported DMA aliases.

use hyper_os::handle::{GuestMemoryObject, OwnedHandle};
use hyper_os::{Error, Result, vm};
use hyper_vm_support::virtqueue::GuestMemory;

pub(super) struct Ram {
    pub handle: OwnedHandle<GuestMemoryObject>,
    pub base: u64,
    pub bytes: u64,
}

impl Ram {
    fn offset(&self, address: u64, bytes: usize) -> Result<u64> {
        let offset = address
            .checked_sub(self.base)
            .ok_or(Error::InvalidResponse)?;
        if offset
            .checked_add(bytes as u64)
            .is_none_or(|end| end > self.bytes)
        {
            return Err(Error::InvalidResponse);
        }
        Ok(offset)
    }
}

impl GuestMemory for Ram {
    type Error = Error;

    fn read(&self, address: u64, bytes: &mut [u8]) -> Result<()> {
        vm::read_guest_memory(
            self.handle.as_handle_ref(),
            self.offset(address, bytes.len())?,
            bytes,
        )
    }

    fn write(&self, address: u64, bytes: &[u8]) -> Result<()> {
        vm::write_guest_memory(
            self.handle.as_handle_ref(),
            self.offset(address, bytes.len())?,
            bytes,
        )
    }

    fn load_u16_acquire(&self, address: u64) -> Result<u16> {
        vm::load_guest_memory(self.handle.as_handle_ref(), self.offset(address, 2)?, 2)
            .map(|value| value as u16)
    }

    fn store_u16_release(&self, address: u64, value: u16) -> Result<()> {
        vm::store_guest_memory(
            self.handle.as_handle_ref(),
            self.offset(address, 2)?,
            2,
            u64::from(value),
        )
    }
}
