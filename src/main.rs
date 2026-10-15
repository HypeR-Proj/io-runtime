// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Supervised lifetime owner for the trusted, idle Linux storage backend.

#[cfg(all(target_os = "hyper", target_arch = "aarch64"))]
mod runtime;

fn main() -> std::process::ExitCode {
    #[cfg(all(target_os = "hyper", target_arch = "aarch64"))]
    return runtime::main();
    #[cfg(not(all(target_os = "hyper", target_arch = "aarch64")))]
    {
        eprintln!("HypeR io-runtime: physical I/O VM requires AArch64");
        std::process::ExitCode::FAILURE
    }
}
