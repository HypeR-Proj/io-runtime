// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Absolute operation budgets, independent of wait readiness.

/// After checking for a completed operation, enforce its original deadline.
/// A ready console/interrupt is not progress and cannot refresh the budget.
#[must_use]
pub fn expired(now: u64, deadline: u64) -> bool {
    now >= deadline
}

#[cfg(test)]
mod tests {
    #[test]
    fn always_ready_unrelated_events_do_not_refresh_operation_budget() {
        let deadline = 60;
        let mut pumps = 0;
        for now in [0, 20, 40, 60, 80] {
            // Each event is ready immediately; the backend reply is absent.
            if super::expired(now, deadline) {
                break;
            }
            pumps += 1;
        }
        assert_eq!(pumps, 3);
        assert!(super::expired(60, deadline));
    }
}
