// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Admission-source failure withdraws new connections, not existing devices.

pub enum Poll<T> {
    Idle,
    Received(T),
    Disabled(hyper_os::Error),
}

pub fn poll<L, T>(
    listener: &mut Option<L>,
    receive: impl FnOnce(&L) -> hyper_os::Result<Option<T>>,
) -> Poll<T> {
    let Some(source) = listener.as_ref() else {
        return Poll::Idle;
    };
    match receive(source) {
        Ok(Some(value)) => Poll::Received(value),
        Ok(None) | Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK)) => Poll::Idle,
        Err(error) => {
            *listener = None;
            Poll::Disabled(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_or_malformed_source_disables_only_future_admissions() {
        for error in [
            hyper_os::Error::Status(hyper_os::Status::PEER_CLOSED),
            hyper_os::Error::InvalidResponse,
        ] {
            let mut listener = Some(());
            assert!(matches!(
                poll::<_, ()>(&mut listener, |_| Err(error)),
                Poll::Disabled(_)
            ));
            assert!(listener.is_none());
            // Terminal source is no longer polled or registered as readable;
            // subsequent service rounds remain available to existing slots.
            assert!(matches!(
                poll::<_, ()>(&mut listener, |_| panic!("disabled source polled")),
                Poll::Idle
            ));
        }
    }

    #[test]
    fn temporary_empty_source_and_valid_admissions_keep_listening() {
        let mut listener = Some(());
        assert!(matches!(
            poll::<_, ()>(&mut listener, |_| Err(hyper_os::Error::Status(
                hyper_os::Status::WOULD_BLOCK
            ))),
            Poll::Idle
        ));
        assert!(listener.is_some());
        assert!(matches!(
            poll(&mut listener, |_| Ok(Some(7))),
            Poll::Received(7)
        ));
        assert!(listener.is_some());
    }
}
