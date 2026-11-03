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
