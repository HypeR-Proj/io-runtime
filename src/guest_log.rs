// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Identify forwarded guest output without buffering an unbounded guest line.

use std::io::{self, Write};

pub struct GuestLog {
    line_start: bool,
}

impl Default for GuestLog {
    fn default() -> Self {
        Self { line_start: true }
    }
}

impl GuestLog {
    pub fn line_start(&self) -> bool {
        self.line_start
    }
    pub fn restore(line_start: bool) -> Self {
        Self { line_start }
    }

    pub fn write(&mut self, output: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
        for part in bytes.split_inclusive(|byte| *byte == b'\n') {
            if self.line_start {
                output.write_all(b"HypeR IO VM: ")?;
                self.line_start = false;
            }
            output.write_all(part)?;
            self.line_start = part.ends_with(b"\n");
        }
        // Partial guest lines must remain visible during early boot and failure.
        output.flush()
    }
}

#[cfg(test)]
#[path = "../tests/guest_log.rs"]
mod tests;
