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
their owners. Pending setup is still serial and has a 60-second deadline.
