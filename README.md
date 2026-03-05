# incus-ts

Lightweight Bun-first TypeScript client for Incus.

The API is instance-handle oriented: get a handle with
`client.instances.instance(name)`, then call `.exec()`, `.setState()`, `.remove()`, etc.

## Mental Model

1. Connect once (`Incus.connect*`).
2. Use collection APIs for listing/creating (`client.instances.*`).
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

const create = await client.instances.create({
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
await create.wait({ timeoutSeconds: 1800 });
```

### 2. Start it

```ts
const instance = client.instances.instance(name);
const start = await instance.setState({ action: "start", timeout: 180 });
await start.wait({ timeoutSeconds: 240 });
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
  const stop = await instance.setState({ action: "stop", timeout: 30, force: true });
  await stop.wait({ timeoutSeconds: 60 });
} catch {
  // Instance might already be stopped.
}

const remove = await instance.remove();
await remove.wait({ timeoutSeconds: 120 });
client.disconnect();
```

### 6. Fork (copy) an instance

```ts
const source = client.instances.instance("my-container");

// Clone current state
const forkOp = await source.fork("my-container-copy");
await forkOp.wait({ timeoutSeconds: 300 });

// Clone from a specific snapshot
const forkFromSnapshotOp = await source.fork("my-container-from-snap", {
  fromSnapshot: "snap0",
});
await forkFromSnapshotOp.wait({ timeoutSeconds: 300 });
```

## Implemented

- `connection`, `raw`, `server`, `operations`
- `images` + `images.aliases` (simple-streams runtime methods are still scaffolded)
- `instances` collection + instance handles (`instances.instance(name)`) with:
  - CRUD/state
  - snapshots (`create`, `list`, `get`, `update`, `rename`, `remove`, `restore`)
  - `exec` (Unix-socket websocket attach, async streaming, promise-style completion)
  - `logs`, `files`, `metadata`, `console`

## Still Scaffolded

- `certificates`, `events`
- `networks`, `profiles`, `projects`
- `storage`, `cluster`, `warnings`
- `instances.templates`, `instances.backups`

## Bun Scripts

- `bun run typecheck`
- `bun run test`
- `bun run test:e2e` (requires local Incus and `INCUS_E2E=1`)
- `bun run build`
- `bun run check`
