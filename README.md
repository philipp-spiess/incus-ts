# incus-ts

Lightweight Bun-first TypeScript client for Incus.

The API is instance-handle oriented: get a handle with
`client.instances.instance(name)`, then call `.exec()`, `.setState()`, `.remove()`, etc.

Operation-returning calls are awaitable by default.

## Mental Model

1. Connect once (`Incus.connect*`).
2. Use collection APIs for listing/creating (`client.instances.*`, `client.networks.*`).
3. Use a per-instance handle for day-to-day work (`client.instances.instance(name)`).

## Quick Start

```ts
import { Incus } from "incus-ts";

const client = await Incus.connectUnix(); // default: /var/lib/incus/unix.socket
const instance = client.instances.instance("my-container");

const proc = instance.exec(
  { command: ["sh", "-lc", "echo hello from incus-ts"], interactive: false },
  { stdout: "pipe", stderr: "pipe" },
);

for await (const chunk of proc) {
  process.stdout.write(new TextDecoder().decode(chunk));
}

const result = await proc;
console.log(result.exitCode, result.ok);
```

## Common Snippets (From E2E Flow)

### 1. Create a container (same base image style as e2e/gondolin setup)

```ts
import { Incus } from "incus-ts";

const client = await Incus.connectUnix();
const name = `incus-ts-demo-${Date.now().toString(36)}`;

await client.instances.create({
  name,
  type: "container",
  source: {
    type: "image",
    mode: "pull",
    server: "https://images.linuxcontainers.org",
    protocol: "simplestreams",
    alias: "alpine/3.20",
  },
});
```

### 2. Start it

```ts
const instance = client.instances.instance(name);
await instance.setState({ action: "start", timeout: 180 });
```

### 3. Stream output while command is running

```ts
const instance = client.instances.instance(name);

const proc = instance.exec(
  { command: ["sh", "-lc", "echo stream:1; cat >/dev/null; echo stream:2"], interactive: false },
  { stdout: "pipe", stderr: "pipe" },
);

for await (const chunk of proc) {
  process.stdout.write(new TextDecoder().decode(chunk));
}

const result = await proc;
console.log(result.exitCode, result.ok);
```

### 4. Run a network check inside the container

```ts
const instance = client.instances.instance(name);
const decoder = new TextDecoder();
let stdout = "";
let stderr = "";

const net = instance.exec(
  {
    command: [
      "sh",
      "-lc",
      "GW=$(ip route | awk '/default/ {print $3; exit}'); "
        + "ping -c 1 -W 2 \"${GW:-192.168.100.1}\" >/tmp/ping.out 2>&1; "
        + "rc=$?; cat /tmp/ping.out; "
        + "if [ \"$rc\" -eq 0 ]; then echo __PING_OK__; fi; "
        + "exit \"$rc\"",
    ],
    interactive: false,
  },
  { stdout: "pipe", stderr: "pipe" },
);

const readStdout = (async () => {
  for await (const chunk of net.stdout) stdout += decoder.decode(chunk, { stream: true });
  stdout += decoder.decode();
})();
const readStderr = (async () => {
  for await (const chunk of net.stderr) stderr += decoder.decode(chunk, { stream: true });
  stderr += decoder.decode();
})();

const netResult = await net;
await Promise.all([readStdout, readStderr]);
if (!netResult.ok || !`${stdout}\n${stderr}`.includes("__PING_OK__")) {
  throw new Error("network check failed");
}
```

### 5. Cleanup

```ts
const instance = client.instances.instance(name);

try {
  await instance.setState({ action: "stop", timeout: 30, force: true });
} catch {
  // Instance might already be stopped.
}

await instance.remove();
client.disconnect();
```

### 6. Snapshot + restore

```ts
const instance = client.instances.instance("my-container");

await instance.snapshots.create({ name: "snap0" });
await instance.restore("snap0");
```

### 7. Fork (copy) an instance

```ts
const source = client.instances.instance("my-container");

// Clone current state
await source.fork("my-container-copy");

// Clone from a specific snapshot
await source.fork("my-container-from-snap", {
  fromSnapshot: "snap0",
});
```

### 8. Discover networks and inspect runtime state

```ts
const networks = await client.networks.list({ filter: ["type=bridge"] });
const network = await client.networks.get("incusbr0");
const state = await client.networks.state("incusbr0");

console.log(networks.map((entry) => entry.name));
console.log(network.value.type);
console.log(state.addresses);
```

### 9. Create and update an OVN network

`networks.update()` follows the rest of the client and performs a `PUT`, so
read the current object first if you want to preserve existing fields.

```ts
const name = "sandbox-ovn";

await client.networks.create({
  name,
  type: "ovn",
  description: "Isolated sandbox network",
  config: {
    network: "none",
    "ipv4.address": "10.42.0.1/24",
    "ipv4.dhcp": "true",
    "ipv4.dhcp.gateway": "10.42.0.2",
    "dns.nameservers": "10.42.0.2",
  },
});

const current = await client.networks.get(name);
await client.networks.update(
  name,
  {
    ...current.value,
    description: "Isolated sandbox network for proxy-routed guests",
  },
  { etag: current.etag },
);
```

### 10. Inspect allocations and leases

```ts
const allocations = await client.networks.allocations();
const leases = await client.networks.leases("sandbox-ovn");

const usedIps = allocations
  .filter((entry) => entry.network === "sandbox-ovn")
  .map((entry) => entry.address)
  .filter((entry): entry is string => typeof entry === "string");

console.log(usedIps);
console.log(leases);
```

### 11. Manage network ACLs

```ts
await client.networks.acls.create({
  name: "proxy-only",
  description: "Only allow traffic via the local proxy appliance",
  ingress: [],
  egress: [],
});

const acl = await client.networks.acls.get("proxy-only");
await client.networks.acls.update(
  "proxy-only",
  {
    ...acl.value,
    description: "Proxy-only egress policy",
  },
  { etag: acl.etag },
);
```

## Implemented

- `connection`, `raw`, `server`, `operations`
- `images` + `images.aliases` (simple-streams runtime methods are still scaffolded)
- `instances` collection + instance handles (`instances.instance(name)`) with:
  - CRUD/state
  - snapshots (`create`, `list`, `get`, `update`, `rename`, `remove`, `restore`)
  - `exec` (Unix-socket websocket attach, async streaming, promise-style completion)
  - `logs`, `files`, `metadata`, `console`
- `networks` with:
  - discovery (`names`, `list`, `get`, `state`)
  - lifecycle (`create`, `update`, `rename`, `remove`)
  - diagnostics (`allocations`, `leases`)
  - ACLs (`networks.acls.*`, including `getLog`)

## Still Scaffolded

- `certificates`, `events`
- `networks.forwards`, `networks.loadBalancers`, `networks.peers`
- `networks.addressSets`, `networks.zones`, `networks.integrations`
- `profiles`, `projects`
- `storage`, `cluster`, `warnings`
- `instances.templates`, `instances.backups`

## Bun Scripts

- `bun run typecheck`
- `bun run test`
- `bun run test:e2e` (requires local Incus and `INCUS_E2E=1`)
- `bun run build`
- `bun run check`
