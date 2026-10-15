// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use hyper_os::{Error, Result, vm};
pub const DEVICE_COOKIE: u64 = 1;
const APERTURE_BYTES: u64 = 65536;

pub(crate) fn line_asserted(status: u32, signal_enable: u32) -> bool {
    status & signal_enable != 0
}

pub(crate) fn request_offset(request: vm::MmioRequest, base: u64) -> Result<u64> {
    let offset = request
        .address
        .checked_sub(base)
        .ok_or(Error::InvalidResponse)?;
    if request.device.get() != DEVICE_COOKIE
        || !matches!(request.width, 1 | 2 | 4)
        || !offset.is_multiple_of(u64::from(request.width))
        || offset
            .checked_add(u64::from(request.width))
            .is_none_or(|end| end > APERTURE_BYTES)
    {
        return Err(Error::InvalidResponse);
    }
    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::num::NonZeroU64;
    const PHYSICAL_MMIO: u64 = 0x0b00_0000;

    #[test]
    fn only_enabled_pending_status_asserts_the_guest_line() {
        assert!(!line_asserted(0, u32::MAX));
        assert!(!line_asserted(1, 2));
        assert!(line_asserted(1, 1));
        assert!(line_asserted(1 << 24, 1 << 24));
        assert!(!line_asserted(u32::MAX, 0));
        // W1C may clear one source while another enabled error remains.
        assert!(line_asserted((1 << 24) | 1, 1 << 24));
    }

    #[test]
    fn mmio_identity_width_alignment_and_aperture_are_checked_before_access() {
        let mut request = vm::MmioRequest {
            id: NonZeroU64::new(1).unwrap(),
            device: NonZeroU64::new(DEVICE_COOKIE).unwrap(),
            address: PHYSICAL_MMIO + 0x30,
            width: 4,
            operation: vm::MmioOperation::Write(1),
        };
        assert_eq!(request_offset(request, PHYSICAL_MMIO), Ok(0x30));
        for (address, width) in [
            (PHYSICAL_MMIO - 1, 1),
            (PHYSICAL_MMIO + APERTURE_BYTES, 4),
            (PHYSICAL_MMIO + 1, 4),
            (PHYSICAL_MMIO, 3),
            (PHYSICAL_MMIO, 8),
        ] {
            request.address = address;
            request.width = width;
            assert!(request_offset(request, PHYSICAL_MMIO).is_err());
        }
        request.address = PHYSICAL_MMIO;
        request.width = 4;
        request.device = NonZeroU64::new(2).unwrap();
        assert!(request_offset(request, PHYSICAL_MMIO).is_err());
    }
}
