// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::GuestLog;

#[test]
fn prefixes_lines_across_arbitrary_read_boundaries() {
    let input = b"Linux boot\n\nHypeR I/O: ready\npartial";
    for chunk in 1..=input.len() {
        let mut log = GuestLog::default();
        let mut output = Vec::new();
        log.write(&mut output, b"").unwrap();
        for part in input.chunks(chunk) {
            log.write(&mut output, part).unwrap();
        }
        assert_eq!(output, b"HypeR IO VM: Linux boot\nHypeR IO VM: \nHypeR IO VM: HypeR I/O: ready\nHypeR IO VM: partial");
    }
}
