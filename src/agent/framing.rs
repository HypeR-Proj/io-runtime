// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Bounded QGA records and the mandatory 0xff synchronization sentinel.

pub const MAX_REPLY: usize = 16 * 1024;

pub struct Decoder {
    bytes: Vec<u8>,
    synchronized: bool,
}

impl Decoder {
    pub fn new() -> Self {
        Self {
            bytes: Vec::new(),
            synchronized: false,
        }
    }

    /// Ignore stale stream fragments until the delimited-sync response starts.
    /// A sentinel also aborts a partial record, as specified by QGA.
    pub fn push(&mut self, byte: u8) -> Result<Option<serde_json::Value>, ()> {
        if byte == 0xff {
            self.bytes.clear();
            self.synchronized = true;
            return Ok(None);
        }
        if !self.synchronized {
            return Ok(None);
        }
        if byte == b'\n' {
            if self.bytes.iter().all(u8::is_ascii_whitespace) {
                self.bytes.clear();
                return Ok(None);
            }
            let result = serde_json::from_slice(&self.bytes).map_err(|_| ());
            self.bytes.clear();
            return result.map(Some);
        }
        if self.bytes.len() == MAX_REPLY {
            return Err(());
        }
        self.bytes.push(byte);
        Ok(None)
    }
}
