<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# io-runtime

Native device and I/O VM transport service. The broker owns device mappings and
existing clients independently of its bounded connection admission worker.

## Workspace tests

Use the [HypeR workspace](https://github.com/HypeR-Proj/HypeR) to resolve dependencies:

```sh
python3 project.py test io-runtime
```

Paths in the documentation are relative to the repo-managed workspace root.
Edit this repository and prepare the workspace again before building.

## Admission failures

Malformed public requests and matched capability-transfer rejections do not
withdraw the broker listener. A failed request-private setup caused by malformed
input, peer closure, timeout or cancellation releases its temporary owners before
any DMA mapping is published. Local invocation and trusted SDK failures disable
future admissions through the broker's diagnostic path; existing devices retain
their owners. Up to eight incomplete memory handshakes run concurrently, each
with its original 60-second deadline. Overload closes the private session before
mapping publication. Observer requests do not consume handshake slots. Completed
results use an eight-entry queue; the main broker remains the sole mapping owner.
Closing a private session reclaims its slot, and listener shutdown cancels and
joins all workers. These are aggregate bounds, not per-client fairness.

A completed admission retains its original handshake deadline while queued. The
broker checks that deadline and session closure before accepting a new binding,
including after waiting for an older binding to retire. Once accepted, backend
transactions, DMA owners and notification routes follow normal ordered
retirement; peer closure alone never proves hardware quiescence.

Closing the public admission endpoint stops new connections without canceling
already-transferred private handshakes. Those sessions retain their finite
budgets; the broker drains their results before observing the public terminal
error. Explicit listener destruction cancels remaining handshakes and joins
the workers. Active backend bindings keep their separate retirement owners.

## Device reactor waits

The device worker retains its host-console wait registration across unrelated
MMIO and IRQ turns. It rearms only a consumed notification and replaces the
registration when the port or its backpressure-dependent interests change.
Peer closure remains subscribed even when reads and writes are both blocked.
Console replacement cancels queued notifications for the old session before
attaching the new one; no notification can target a replacement port.

## Initialized random service

A deployment may declare that this storage successor accepts the optional
`random` service endpoint. System startup creates its capability pair before
starting interactive services, then moves the provider endpoint during adoption.
Clients receive only `WRITE|WAIT` plus explicit delegation rights; the provider
receives `READ|WAIT`. The kernel does not generate random bytes.

The trusted Linux I/O appliance supplies 1–192 initialized bytes per owner
transaction using nonblocking `getrandom`. Missing protocol support returns
`NOT_SUPPORTED`; an uninitialized source returns `WOULD_BLOCK`. Native code does
not seed a second generator or substitute timestamps, addresses or counters.
Binding, epoch, advertised features and the last owner transaction survive the
bootstrap handoff. Random results are never replayed or logged.

Admission uses one persistent receiver and a queue of eight requests, with one
owner transaction in flight. Client deadlines remain absolute. A canceled sent
request is drained before another transaction starts. Five-second health limits
bound owner-mailbox send admission and, separately, reply completion; expiration
reports `IO_ERROR` and permanently disables that control session. Thus one
active request can occupy at most approximately ten seconds across both phases,
or less when its caller deadline expires. A late reply is
discarded without reopening it. Established block queues continue running.
Replies to clients never block the main I/O event loop. The listener uses 250 ms
cancellable receive slices; shutdown closes its bounded queue before joining.

On Pi5 the bootstrap owner explicitly assigns the firmware-discovered RNG200
register bank to the I/O VM through the existing MMIO consumer. Linux owns its
hardware driver, entropy conditioning, readiness and reseeding. QEMU acceptance
uses an RNDR-capable CPU. An unsupported platform or appliance fails explicitly;
passing byte-format tests is not a cryptographic assessment of an entropy source.
