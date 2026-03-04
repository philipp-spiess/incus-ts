# incus-ts

First step of a Bun-first TypeScript port of the Incus Go client.

Current status: core runtime is implemented for transport, raw requests, server,
operations, images, and a practical subset of instance operations. Other
domains are scaffolded and currently return explicit "not implemented yet"
errors.

## Goals

- Keep setup lightweight (Bun for install/build/test).
- Preserve Incus Go client capability domains.
- Provide a Gondolin-like ergonomic TypeScript surface:
  - static factories (`Incus.connect*`)
  - grouped resource APIs (`client.instances`, `client.networks`, ...)
  - chainable context scoping (`client.project(...).target(...)`)

## Quick look

```ts
import { Incus } from "incus-ts";

const client = await Incus.connect("https://incus.example");

const scoped = client.project("my-project").target("node-1");

await scoped.instances.list({ type: "container", allProjects: false });
await scoped.images.aliases.get("alpine/3.20");
```

## Implemented now

- `connection`, `raw`
- `server`
- `operations`
- `images` + `images.aliases` (simple streams remains unimplemented)
- `instances` core CRUD/state/exec/console/metadata + `instances.logs` + `instances.files`
  with websocket exec stream attach over Unix sockets

## Sketched but not implemented yet

- `certificates`
- `events`
- `networks` and nested groups
- `profiles`, `projects`
- `storage` and nested groups
- `cluster`
- `warnings`
- `instances.templates`, `instances.snapshots`, `instances.backups`

## Scripts

- `bun run typecheck`
- `bun run test`
- `bun run test:e2e` (requires local Incus and `INCUS_E2E=1`)
- `bun run build`
- `bun run check`
