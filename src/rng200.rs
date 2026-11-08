// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Bounded register forwarding only; the Linux rng200 driver owns all device policy.
use hyper_os::handle::{HandleRef, PhysicalDeviceObject};
use hyper_os::{Error, Result, device, vm};
pub const BASE: u64 = 0x0b02_0000;
pub const BYTES: u64 = 0x28;
pub const APERTURE: u64 = 65536;
pub const COOKIE: u64 = 0x524e_4732;
/// Reserved assignment line; the device has no physical or guest interrupt.
pub const RESERVED_IRQ: u32 = 122;

fn offset(request: vm::MmioRequest) -> Result<u64> {
    let offset = request
        .address
        .checked_sub(BASE)
        .ok_or(Error::InvalidResponse)?;
    let allowed = match request.operation {
        vm::MmioOperation::Read => matches!(offset, 0 | 4 | 8 | 0x18 | 0x20 | 0x24),
        vm::MmioOperation::Write(_) => matches!(offset, 0 | 4 | 8 | 0x18),
    };
    if request.device.get() != COOKIE || request.width != 4 || !allowed {
        return Err(Error::InvalidResponse);
    }
    Ok(offset)
}

pub fn forward(
    physical: HandleRef<'_, PhysicalDeviceObject>,
    request: vm::MmioRequest,
) -> Result<vm::MmioCompletion> {
    let offset = offset(request)?;
    match request.operation {
        vm::MmioOperation::Read => Ok(vm::MmioCompletion::Read(device::mmio_read(
            physical, offset, 4,
        )?)),
        vm::MmioOperation::Write(value) => {
            device::mmio_write(physical, offset, 4, value)?;
            Ok(vm::MmioCompletion::Write)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::num::NonZeroU64;
    #[test]
    fn only_documented_u32_register_operations_are_forwarded() {
        let mut r = vm::MmioRequest {
            id: NonZeroU64::new(1).unwrap(),
            device: NonZeroU64::new(COOKIE).unwrap(),
            address: BASE,
            width: 4,
            operation: vm::MmioOperation::Read,
        };
        for register in [0, 4, 8, 0x18, 0x20, 0x24] {
            r.address = BASE + register;
            assert_eq!(offset(r), Ok(register));
        }
        for register in [1, 0xc, 0x10, 0x1c, 0x28, APERTURE] {
            r.address = BASE + register;
            assert!(offset(r).is_err());
        }
        r.address = BASE + 0x24;
        r.operation = vm::MmioOperation::Write(1);
        assert!(offset(r).is_err());
        r.address = BASE;
        r.width = 8;
        assert!(offset(r).is_err());
        r.width = 4;
        r.device = NonZeroU64::new(COOKIE + 1).unwrap();
        assert!(offset(r).is_err());
    }
}
