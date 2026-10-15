// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn offer() -> Description {
    let kinds = [
        Kind::Machine,
        Kind::Cpu,
        Kind::Serial,
        Kind::SerialBuffer,
        Kind::OwnMemory,
        Kind::HotplugInterrupt,
        Kind::StorageDevice,
        Kind::Broker,
        Kind::NativeMemory,
        Kind::NativeMailbox,
        Kind::BrokerMailbox,
    ];
    Description {
        version: 2,
        memory_base: 0x4000_0000,
        memory_size: 64 * 1024 * 1024,
        name: "io".into(),
        image: "/vm/io.itb".into(),
        worker: WorkerMode::None,
        cpus: 1,
        native: 1,
        network: false,
        broker: true,
        clients: vec![Client {
            id: 1,
            volume: Some("disk".into()),
            network: None,
            mac: [0; 6],
        }],
        slots: kinds
            .into_iter()
            .map(|kind| Slot {
                kind,
                rights: kind.required_rights().bits(),
            })
            .collect(),
    }
}

#[test]
fn description_preserves_exact_role_order_and_rejects_missing_or_extra_owners() {
    let original = offer();
    assert_eq!(original.validate(), Ok(()));
    let encoded = serde_json::to_vec(&original).unwrap();
    let decoded: Description = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded.validate(), Ok(()));
    for index in 0..original.slots.len() {
        let mut missing = original.clone();
        missing.slots.remove(index);
        assert!(missing.validate().is_err());
        let mut duplicate = original.clone();
        duplicate.slots.insert(index, original.slots[index].clone());
        assert!(duplicate.validate().is_err());
    }
    let mut reordered = original;
    reordered.slots.swap(1, 2);
    assert!(reordered.validate().is_err());
}

#[test]
fn malformed_or_attenuated_inventories_are_rejected_before_live_receive() {
    let original = offer();
    for index in 0..original.slots.len() {
        let mut missing_rights = original.clone();
        missing_rights.slots[index].rights = 0;
        assert!(missing_rights.validate().is_err());
    }
    let mut wrong_version = original.clone();
    wrong_version.version = 1;
    assert!(wrong_version.validate().is_err());
    let mut unavailable_worker = original.clone();
    unavailable_worker.worker = WorkerMode::Sdhci;
    assert!(unavailable_worker.validate().is_err());
    let mut duplicate_client = original.clone();
    duplicate_client.clients.push(original.clients[0].clone());
    duplicate_client
        .slots
        .push(original.slots.last().unwrap().clone());
    assert!(duplicate_client.validate().is_err());
    let mut excessive_cpu = original;
    excessive_cpu.cpus = 9;
    assert!(excessive_cpu.validate().is_err());
}

#[test]
fn client_media_and_network_exclusivity_survive_handoff() {
    let mut original = offer();
    let mut second = original.clients[0].clone();
    second.id = 2;
    original.clients.push(second);
    original.slots.push(Slot {
        kind: Kind::BrokerMailbox,
        rights: Kind::BrokerMailbox.required_rights().bits(),
    });
    // Distinct client IDs still cannot claim the same media.
    assert!(original.validate().is_err());
    original.clients[1].volume = Some("other-disk".into());
    assert_eq!(original.validate(), Ok(()));

    original.clients[0].network = Some("default".into());
    original.clients[0].mac = [0x02, 0x48, 0x59, 0, 0, 1];
    original.clients[1].network = Some("default".into());
    original.clients[1].mac = original.clients[0].mac;
    assert!(original.validate().is_err());
    original.clients[1].mac[5] = 2;
    assert_eq!(original.validate(), Ok(()));
}

#[test]
fn unknown_fields_are_rejected_at_every_handoff_record_level()
-> Result<(), Box<dyn std::error::Error>> {
    let original = serde_json::to_value(offer())?;
    for record in ["description", "slot", "client"] {
        let mut value = original.clone();
        let target = match record {
            "slot" => &mut value["slots"][0],
            "client" => &mut value["clients"][0],
            _ => &mut value,
        };
        target["unsupported"] = serde_json::Value::Bool(true);
        assert!(
            serde_json::from_value::<Description>(value).is_err(),
            "{record}"
        );
    }
    Ok(())
}

#[test]
fn partial_serial_line_and_full_width_cursor_survive_commit_encoding() {
    for cursor in [0, 1, u32::MAX as u64, u64::MAX] {
        for line_start in [true, false] {
            let bridge = hyper_vm_support::pcie::RootPort::default();
            let record = encode_commit(cursor, line_start, &bridge);
            assert_eq!(decode_commit(&record), Ok((cursor, line_start, bridge)));
            for length in 0..record.len() {
                assert!(decode_commit(&record[..length]).is_err());
            }
            let mut extra = record.clone();
            extra.push(0);
            assert!(decode_commit(&extra).is_err());
            let mut bad_boolean = record;
            bad_boolean[ADOPT_MESSAGE.len() + 8] = 2;
            assert!(decode_commit(&bad_boolean).is_err());
        }
    }
}

#[test]
fn committed_empty_slot_retains_guest_controls_and_rejects_an_inserted_device() {
    let mut port = hyper_vm_support::pcie::RootPort::default();
    assert_eq!(port.write(4, 2, 6), Some(()));
    assert_eq!(port.write(0x58, 2, 0x1028), Some(()));
    assert_eq!(port.write(0x20, 4, 0x0ab0_0aa0), Some(()));
    let record = encode_commit(99, false, &port);
    assert_eq!(decode_commit(&record), Ok((99, false, port.clone())));
    port.set_present(true);
    assert!(decode_commit(&encode_commit(99, false, &port)).is_err());
}
