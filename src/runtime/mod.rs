// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! System ownership of an existing I/O VM and business-client policy.

mod adopt;
#[cfg(not(feature = "console-test"))]
mod agent;
mod broker;
mod faults;

use hyper_io_runtime_system::guest::install::InstalledGuest;
use hyper_io_runtime_system::guest_log::GuestLog;
use hyper_io_runtime_system::handoff;
use hyper_os::handle::{ByteChannelObject, GuestMailboxObject, VirtualMachineObject};
use hyper_os::startup::StartupPurpose;
use hyper_os::vm;
use hyper_os::wait::{self, ObjectSignals, WaitItem};
use std::io;
use std::process::ExitCode;
use std::time::Duration;

type Result<T> = std::result::Result<T, String>;
fn show(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}
fn deadline(seconds: u64) -> Result<u64> {
    hyper_os::time::deadline_after(Duration::from_secs(seconds))
        .map(|deadline| deadline.as_raw())
        .map_err(show)
}
fn check_deadline(limit: u64) -> Result<()> {
    let now = hyper_os::time::monotonic_now()
        .map_err(show)?
        .as_nanoseconds();
    if hyper_io_runtime_system::deadline::expired(now, limit) {
        Err("I/O operation timed out".into())
    } else {
        Ok(())
    }
}

pub(super) fn main() -> ExitCode {
    let result = (|| -> Result<()> {
        let mut startup = hyper_rt::process::startup().map_err(show)?;
        let readiness = startup
            .take(StartupPurpose::<ByteChannelObject>::new(handoff::READY))
            .map_err(show)?;
        let mut owner = adopt::receive(&mut startup)?;
        // Ownership has already moved. A timed-out supervisor may have closed
        // its readiness endpoint; that must not stop the running I/O VM or
        // release the backing still used by filesystem and guest clients.
        if let Err(error) = readiness.as_byte_channel().send(handoff::READY_MESSAGE) {
            eprintln!("HypeR io-runtime: adopted backend readiness unavailable: {error:?}");
        }
        println!("HypeR io-runtime: running backend adopted");
        let outcome = serve(&mut owner);
        let stopped = vm::request_stop(owner.guest.machine.as_handle_ref())
            .map_err(show)
            .and_then(|()| {
                vm::wait_terminated(owner.guest.machine.as_handle_ref(), deadline(20)?)
                    .map_err(show)
            });
        let worker = owner
            .worker
            .take()
            .map(|worker| worker.stop().map_err(show))
            .transpose()
            .map(|_| ());
        let drained = drain(&mut owner.guest, &mut owner.log);
        outcome.and(stopped).and(worker).and(drained)
    })();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("HypeR io-runtime: failed: {error}");
            ExitCode::FAILURE
        }
    }
}
fn serve(owner: &mut adopt::Adopted) -> Result<()> {
    let mut faults = faults::Monitor::new();
    #[cfg(not(feature = "console-test"))]
    let mut agent = agent::Agent::new(hyper_io_runtime_system::guest::install::RAM_BYTES);
    loop {
        let now = hyper_os::time::monotonic_now()
            .map_err(show)?
            .as_nanoseconds()
            / 1_000_000;
        faults.poll(owner, now);
        if !pump_guest(&mut owner.guest, &mut owner.log)? {
            return Ok(());
        }
        #[cfg(not(feature = "console-test"))]
        let agent_progress = owner
            .worker
            .as_ref()
            .is_some_and(|worker| agent.poll(worker, now));
        #[cfg(feature = "console-test")]
        let agent_progress = false;
        #[cfg(not(feature = "console-test"))]
        let metrics = agent.metrics(now);
        #[cfg(feature = "console-test")]
        let metrics = hyper_service::io::metrics::GuestMetrics::default();
        let broker_progress = if let Some(broker) = &mut owner.broker {
            broker.service(&mut owner.guest, metrics)?
        } else {
            false
        };
        if agent_progress || broker_progress {
            continue;
        }
        let mut items = vec![
            owner.guest.output.wait_item(),
            WaitItem::new(
                owner.guest.machine.as_handle_ref(),
                ObjectSignals::<VirtualMachineObject>::POWER_REQUEST
                    .union(ObjectSignals::<VirtualMachineObject>::VCPU_TERMINATED),
            ),
        ];
        let mailbox = owner
            .native_mailboxes
            .first()
            .ok_or("missing management mailbox")?;
        items.push(WaitItem::new(
            mailbox.as_handle_ref(),
            ObjectSignals::<GuestMailboxObject>::READABLE
                .union(ObjectSignals::<GuestMailboxObject>::PEER_CLOSED),
        ));
        if let Some(broker) = &owner.broker {
            items.extend(broker.wait_items());
        }
        #[cfg(not(feature = "console-test"))]
        if let Some(item) = agent.wait_item() {
            items.push(item);
        }
        let limit = owner
            .broker
            .as_ref()
            .map_or(hyper_os::DEADLINE_INFINITE, |broker| broker.next_deadline());
        #[cfg(not(feature = "console-test"))]
        let limit = limit.min(agent.deadline());
        faults.append_waits(owner, &mut items);
        match wait::wait_many(&items, limit) {
            Ok(event) if event.index == 2 => {
                return Err("unexpected idle backend control event".into());
            }
            Ok(event) => faults.wake(event.index),
            Err(hyper_os::Error::Status(hyper_os::Status::TIMED_OUT)) => (),
            Err(error) => return Err(show(error)),
        }
    }
}
fn drain(guest: &mut InstalledGuest, guest_log: &mut GuestLog) -> Result<()> {
    let mut bytes = [0; 2048];
    let mut output = io::stdout().lock();
    for _ in 0..16 {
        let length = guest.output.try_read(&mut bytes).map_err(show)?;
        if length == 0 {
            break;
        }
        guest_log
            .write(&mut output, &bytes[..length])
            .map_err(show)?;
    }
    Ok(())
}

fn pump_guest(guest: &mut InstalledGuest, guest_log: &mut GuestLog) -> Result<bool> {
    drain(guest, guest_log)?;
    for cpu in &guest.cpus {
        if let Some(reason) = vm::vcpu_info(cpu.as_handle_ref()).map_err(show)?.terminal {
            return Err(format!("I/O vCPU terminated: {reason:?}"));
        }
    }
    for _ in 0..guest.cpus.len() {
        let Some(request) =
            vm::pending_power_request(guest.machine.as_handle_ref()).map_err(show)?
        else {
            break;
        };
        match request.operation {
            vm::PowerOperation::CpuOn | vm::PowerOperation::CpuOff => {
                vm::complete_power_request(guest.machine.as_handle_ref(), request.id, true)
                    .map_err(show)?
            }
            vm::PowerOperation::SystemOff | vm::PowerOperation::SystemReset => {
                eprintln!("HypeR io-runtime: backend requested shutdown");
                return Ok(false);
            }
        }
    }
    Ok(true)
}
