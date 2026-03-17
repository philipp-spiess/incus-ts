import { expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Incus, IncusClient, IncusImageClient } from "../src/index";
import type {
  IncusTransport,
  IncusTransportRequestOptions,
  IncusTransportResponse,
} from "../src/index";

class FakeTransport implements IncusTransport {
  readonly calls: Array<{
    method: string;
    path: string;
    options?: IncusTransportRequestOptions;
  }> = [];
  private readonly routes = new Map<
    string,
    (
      options: IncusTransportRequestOptions | undefined,
      method: string,
      path: string,
    ) => IncusTransportResponse<unknown> | Promise<IncusTransportResponse<unknown>>
  >();
  private readonly websocketRoutes = new Map<string, () => WebSocket>();

  on(
    method: string,
    path: string,
    handler: (
      options: IncusTransportRequestOptions | undefined,
      method: string,
      path: string,
    ) => IncusTransportResponse<unknown> | Promise<IncusTransportResponse<unknown>>,
  ) {
    this.routes.set(`${method.toUpperCase()} ${path}`, handler);
  }

  onWebsocket(path: string, socket: WebSocket | (() => WebSocket)) {
    const factory = typeof socket === "function" ? socket : () => socket;
    this.websocketRoutes.set(path, factory);
  }

  async request<T = unknown>(
    method: string,
    path: string,
    options?: IncusTransportRequestOptions,
  ): Promise<IncusTransportResponse<T>> {
    this.calls.push({ method, path, options });
    const route = this.routes.get(`${method.toUpperCase()} ${path}`);
    if (!route) {
      throw new Error(`Missing fake route for ${method.toUpperCase()} ${path}`);
    }

    return (await route(options, method, path)) as IncusTransportResponse<T>;
  }

  async websocket(path: string): Promise<WebSocket> {
    const route = this.websocketRoutes.get(path);
    if (!route) {
      throw new Error(`Missing fake websocket route for ${path}`);
    }

    return route();
  }
}

class FakeWebSocket {
  private readonly events = new EventEmitter();
  readonly sentMessages: unknown[] = [];
  closedCount = 0;

  on(type: string, listener: (...args: unknown[]) => void) {
    this.events.on(type, listener);
  }

  off(type: string, listener: (...args: unknown[]) => void) {
    this.events.off(type, listener);
  }

  send(data: unknown) {
    this.sentMessages.push(data);
  }

  close() {
    this.closedCount += 1;
    this.events.emit("close");
  }

  emitMessage(data: Uint8Array | string) {
    this.events.emit("message", data);
  }

  emitClose() {
    this.events.emit("close");
  }

  emitError(error: Error) {
    this.events.emit("error", error);
  }
}

test("creates a typed Incus client with grouped APIs", async () => {
  const client = await Incus.connect("https://incus.example.internal/");

  expect(client).toBeInstanceOf(IncusClient);
  expect(client.endpoint).toBe("https://incus.example.internal");
  expect(typeof client.instances.list).toBe("function");
  expect(typeof client.instances.instance("demo").exec).toBe("function");
  expect(typeof client.networks.zones.records.get).toBe("function");

  const scoped = client.project("tenant-a").target("node-2").requireAuthenticated();
  expect(scoped.context.project).toBe("tenant-a");
  expect(scoped.context.target).toBe("node-2");
  expect(scoped.context.requireAuthenticated).toBe(true);
});

test("creates image-only clients for public/simple-streams endpoints", async () => {
  const publicClient = await Incus.connectPublic("https://images.example.com/");
  const simpleStreamsClient = await Incus.connectSimpleStreams(
    "https://images.linuxcontainers.org/",
  );

  expect(publicClient).toBeInstanceOf(IncusImageClient);
  expect(simpleStreamsClient).toBeInstanceOf(IncusImageClient);
  expect(typeof publicClient.images.aliases.get).toBe("function");
});

test("propagates project/target context and parses operation ids", async () => {
  const transport = new FakeTransport();
  transport.on("GET", "/1.0/instances", (options) => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: ["/1.0/instances/c1"],
    },
    headers: new Headers(),
  }));
  transport.on("POST", "/1.0/instances", () => ({
    status: 200,
    data: {
      type: "async",
      status: "Operation created",
      status_code: 100,
      operation: "/1.0/operations/op-123",
      metadata: {},
    },
    headers: new Headers(),
  }));
  transport.on("GET", "/1.0/operations/op-123/wait", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: { done: true },
    },
    headers: new Headers(),
  }));

  const client = Incus.fromTransport("https://incus.example.internal", transport)
    .project("tenant-a")
    .target("node-2")
    .requireAuthenticated(true);

  const names = await client.instances.names({ type: "container" });
  expect(names).toEqual(["c1"]);

  const operation = client.instances.create({ name: "c1" });
  expect(typeof operation.wait).toBe("function");
  expect(await operation).toEqual({ done: true });

  expect(transport.calls[0]?.options?.query).toEqual({ "instance-type": "container" });
  expect(transport.calls[0]?.options?.context).toEqual({
    project: "tenant-a",
    target: "node-2",
    requireAuthenticated: true,
  });
});

test("raw.query bypasses scoped project/target context", async () => {
  const transport = new FakeTransport();
  transport.on("GET", "/1.0", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: { api_extensions: ["api_filtering"] },
    },
    headers: new Headers(),
  }));

  const client = Incus.fromTransport("https://incus.example.internal", transport)
    .project("tenant-a")
    .target("node-2");

  const response = await client.raw.query("GET", "/1.0");
  expect(response.value).toEqual({ api_extensions: ["api_filtering"] });
  expect(transport.calls[0]?.options?.context).toBeUndefined();
});

test("instances.exec supports pipe streaming with async iterators", async () => {
  const transport = new FakeTransport();
  const stdoutSocket = new FakeWebSocket();
  const stderrSocket = new FakeWebSocket();
  const decoder = new TextDecoder();

  transport.on("POST", "/1.0/instances/c1/exec", () => ({
    status: 200,
    data: {
      type: "async",
      status: "Operation created",
      status_code: 100,
      operation: "/1.0/operations/op-exec-1",
      metadata: {
        fds: {
          "1": "sec-out",
          "2": "sec-err",
        },
      },
    },
    headers: new Headers(),
  }));
  transport.on("GET", "/1.0/operations/op-exec-1/wait", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: {
        status: "Success",
        status_code: 200,
        metadata: {
          return: 0,
        },
      },
    },
    headers: new Headers(),
  }));
  transport.onWebsocket("/1.0/operations/op-exec-1/websocket?secret=sec-out", () => (
    stdoutSocket as unknown as WebSocket
  ));
  transport.onWebsocket("/1.0/operations/op-exec-1/websocket?secret=sec-err", () => (
    stderrSocket as unknown as WebSocket
  ));

  const client = Incus.fromTransport("https://incus.example.internal", transport);
  const proc = client.instances.instance("c1").exec(
    { command: ["/bin/sh", "-lc", "echo hi"] },
    { stdout: "pipe", stderr: "pipe" },
  );

  const stdoutRead = (async () => {
    const chunks: Uint8Array[] = [];
    for await (const chunk of proc) {
      chunks.push(chunk);
    }

    return decoder.decode(Buffer.concat(chunks.map((chunk) => Buffer.from(chunk))));
  })();

  const mergedRead = (async () => {
    const entries: Array<{ stream: string; text: string }> = [];
    for await (const entry of proc.output()) {
      entries.push({ stream: entry.stream, text: decoder.decode(entry.chunk) });
    }

    return entries;
  })();

  for (let attempt = 0; attempt < 20 && proc.id.length === 0; attempt += 1) {
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
  }
  expect(proc.id).toBe("op-exec-1");

  stdoutSocket.emitMessage(new TextEncoder().encode("hello\n"));
  stderrSocket.emitMessage(new TextEncoder().encode("oops\n"));
  stdoutSocket.emitClose();
  stderrSocket.emitClose();

  const result = await proc;
  expect(result.ok).toBe(true);
  expect(result.exitCode).toBe(0);
  expect(await stdoutRead).toBe("hello\n");
  expect(await mergedRead).toEqual([
    { stream: "stdout", text: "hello\n" },
    { stream: "stderr", text: "oops\n" },
  ]);
});

test("instances.exec keeps streaming stdin over callback-less node-style websockets", async () => {
  const transport = new FakeTransport();
  const stdinSocket = new FakeWebSocket();
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  let stdinController: ReadableStreamDefaultController<Uint8Array> | undefined;

  transport.on("POST", "/1.0/instances/c1/exec", () => ({
    status: 200,
    data: {
      type: "async",
      status: "Operation created",
      status_code: 100,
      operation: "/1.0/operations/op-exec-stdin",
      metadata: {
        fds: {
          "0": "sec-in",
        },
      },
    },
    headers: new Headers(),
  }));
  transport.onWebsocket("/1.0/operations/op-exec-stdin/websocket?secret=sec-in", () => (
    stdinSocket as unknown as WebSocket
  ));

  const stdin = new ReadableStream<Uint8Array>({
    start(controller) {
      stdinController = controller;
    },
  });

  const client = Incus.fromTransport("https://incus.example.internal", transport);
  const proc = client.instances.instance("c1").exec(
    { command: ["/bin/cat"] },
    { stdin },
  );

  for (let attempt = 0; attempt < 20 && proc.id.length === 0; attempt += 1) {
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
  }
  expect(proc.id).toBe("op-exec-stdin");

  stdinController?.enqueue(encoder.encode("{\"jsonrpc\":\"2.0\",\"id\":1}\n"));
  stdinController?.enqueue(encoder.encode("{\"jsonrpc\":\"2.0\",\"id\":2}\n"));
  stdinController?.close();

  for (
    let attempt = 0;
    attempt < 20 && (stdinSocket.sentMessages.length < 2 || stdinSocket.closedCount === 0);
    attempt += 1
  ) {
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
  }

  expect(
    stdinSocket.sentMessages.map((message) => (
      decoder.decode(Buffer.from(message as Uint8Array))
    )),
  ).toEqual([
    "{\"jsonrpc\":\"2.0\",\"id\":1}\n",
    "{\"jsonrpc\":\"2.0\",\"id\":2}\n",
  ]);
  expect(stdinSocket.closedCount).toBeGreaterThan(0);
});

test("instance snapshots support lifecycle and restore", async () => {
  const transport = new FakeTransport();

  transport.on("GET", "/1.0/instances/c1/snapshots", (options) => {
    if (String(options?.query?.recursion ?? "") === "1") {
      return {
        status: 200,
        data: {
          type: "sync",
          status: "Success",
          status_code: 200,
          metadata: [{ name: "snap0" }],
        },
        headers: new Headers(),
      };
    }

    return {
      status: 200,
      data: {
        type: "sync",
        status: "Success",
        status_code: 200,
        metadata: ["/1.0/instances/c1/snapshots/snap0"],
      },
      headers: new Headers(),
    };
  });

  transport.on("GET", "/1.0/instances/c1/snapshots/snap0", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: { name: "snap0" },
    },
    headers: new Headers(),
  }));

  transport.on("POST", "/1.0/instances/c1/snapshots", () => ({
    status: 200,
    data: {
      type: "async",
      status: "Operation created",
      status_code: 100,
      operation: "/1.0/operations/op-snap-create",
      metadata: {},
    },
    headers: new Headers(),
  }));

  transport.on("POST", "/1.0/instances/c1/snapshots/snap0", () => ({
    status: 200,
    data: {
      type: "async",
      status: "Operation created",
      status_code: 100,
      operation: "/1.0/operations/op-snap-post",
      metadata: {},
    },
    headers: new Headers(),
  }));

  transport.on("PUT", "/1.0/instances/c1/snapshots/snap0", () => ({
    status: 200,
    data: {
      type: "async",
      status: "Operation created",
      status_code: 100,
      operation: "/1.0/operations/op-snap-update",
      metadata: {},
    },
    headers: new Headers(),
  }));

  transport.on("DELETE", "/1.0/instances/c1/snapshots/snap0", () => ({
    status: 200,
    data: {
      type: "async",
      status: "Operation created",
      status_code: 100,
      operation: "/1.0/operations/op-snap-delete",
      metadata: {},
    },
    headers: new Headers(),
  }));

  transport.on("PUT", "/1.0/instances/c1", (options) => {
    const body = options?.body as Record<string, unknown> | undefined;
    const operationId = body?.stateful ? "op-restore-stateful" : "op-restore";
    return {
      status: 200,
      data: {
        type: "async",
        status: "Operation created",
        status_code: 100,
        operation: `/1.0/operations/${operationId}`,
        metadata: {},
      },
      headers: new Headers(),
    };
  });

  const client = Incus.fromTransport("https://incus.example.internal", transport);
  const instance = client.instances.instance("c1");

  expect(await instance.snapshots.names()).toEqual(["snap0"]);
  expect(await instance.snapshots.list()).toEqual([{ name: "snap0" }]);
  expect((await instance.snapshots.get("snap0")).value).toEqual({ name: "snap0" });

  const snapshotCreate = instance.snapshots.create({ name: "snap0" });
  const snapshotRename = instance.snapshots.rename("snap0", { name: "snap1" });
  const snapshotMigrate = instance.snapshots.migrate("snap0", { migration: true });
  const snapshotUpdate = instance.snapshots.update("snap0", { expires_at: "2030-01-01T00:00:00Z" });
  const snapshotRemove = instance.snapshots.remove("snap0");
  expect(typeof snapshotCreate.wait).toBe("function");
  expect(typeof snapshotRename.wait).toBe("function");
  expect(typeof snapshotMigrate.wait).toBe("function");
  expect(typeof snapshotUpdate.wait).toBe("function");
  expect(typeof snapshotRemove.wait).toBe("function");

  const restoreA = instance.restore("snap0");
  const restoreB = instance.snapshots.restore("snap0", { stateful: true });
  expect(typeof restoreA.wait).toBe("function");
  expect(typeof restoreB.wait).toBe("function");

  const restoreCalls = transport.calls.filter(
    (call) => call.method.toUpperCase() === "PUT" && call.path === "/1.0/instances/c1",
  );
  expect(restoreCalls[0]?.options?.body).toEqual({ restore: "snap0" });
  expect(restoreCalls[1]?.options?.body).toEqual({ restore: "snap0", stateful: true });
});

test("instance.fork creates copy requests and supports direct await completion", async () => {
  const transport = new FakeTransport();
  transport.on("POST", "/1.0/instances", () => ({
    status: 200,
    data: {
      type: "async",
      status: "Operation created",
      status_code: 100,
      operation: "/1.0/operations/op-fork",
      metadata: {},
    },
    headers: new Headers(),
  }));
  transport.on("GET", "/1.0/operations/op-fork/wait", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: {
        status: "Success",
        status_code: 200,
      },
    },
    headers: new Headers(),
  }));

  const client = Incus.fromTransport("https://incus.example.internal", transport);
  const instance = client.instances.instance("source");

  const forkA = instance.fork("clone-a");
  const forkB = instance.fork("clone-b", {
    fromSnapshot: "snap0",
    sourceProject: "other-project",
    live: true,
    instanceOnly: true,
    refresh: true,
    refreshExcludeOlder: true,
    allowInconsistent: true,
  });

  expect(await forkA).toEqual({
    status: "Success",
    status_code: 200,
  });
  expect(await forkB).toEqual({
    status: "Success",
    status_code: 200,
  });

  const postCalls = transport.calls.filter(
    (call) => call.method.toUpperCase() === "POST" && call.path === "/1.0/instances",
  );
  const waitCalls = transport.calls.filter(
    (call) => call.method.toUpperCase() === "GET" && call.path === "/1.0/operations/op-fork/wait",
  );

  expect(postCalls).toHaveLength(2);
  expect(waitCalls).toHaveLength(2);
  expect(waitCalls[0]?.options?.query).toEqual({ timeout: -1 });
  expect(waitCalls[1]?.options?.query).toEqual({ timeout: -1 });
  expect(postCalls[0]?.options?.body).toEqual({
    name: "clone-a",
    source: {
      type: "copy",
      source: "source",
    },
  });
  expect(postCalls[1]?.options?.body).toEqual({
    name: "clone-b",
    source: {
      type: "copy",
      source: "source/snap0",
      project: "other-project",
      live: true,
      instance_only: true,
      refresh: true,
      refresh_exclude_older: true,
      allow_inconsistent: true,
    },
  });
});

test("networks discovery methods use expected paths, queries, and shapes", async () => {
  const transport = new FakeTransport();

  transport.on("GET", "/1.0/networks", (options) => {
    if (String(options?.query?.recursion ?? "") === "1") {
      return {
        status: 200,
        data: {
          type: "sync",
          status: "Success",
          status_code: 200,
          metadata: [{ name: "incusbr0", type: "bridge" }],
        },
        headers: new Headers(),
      };
    }

    return {
      status: 200,
      data: {
        type: "sync",
        status: "Success",
        status_code: 200,
        metadata: [
          "/1.0/networks/incusbr0",
          "/1.0/networks/uplink",
        ],
      },
      headers: new Headers(),
    };
  });
  transport.on("GET", "/1.0/networks/incusbr0", () => ({
    status: 200,
    etag: "network-etag",
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: {
        name: "incusbr0",
        type: "bridge",
      },
    },
    headers: new Headers(),
  }));
  transport.on("GET", "/1.0/networks/incusbr0/state", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: {
        hwaddr: "00:16:3e:ab:cd:ef",
        mtu: 1500,
        addresses: [{ family: "inet", address: "10.10.10.1" }],
      },
    },
    headers: new Headers(),
  }));
  transport.on("GET", "/1.0/networks/incusbr0/leases", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: [
        {
          hostname: "vm-a",
          address: "10.10.10.42",
          hwaddr: "00:16:3e:aa:bb:cc",
        },
      ],
    },
    headers: new Headers(),
  }));
  transport.on("GET", "/1.0/network-allocations", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: [
        {
          project: "default",
          used_by: "/1.0/instances/vm-a",
          address: "10.10.10.42",
        },
      ],
    },
    headers: new Headers(),
  }));

  const client = Incus.fromTransport("https://incus.example.internal", transport);

  expect(await client.networks.names()).toEqual(["incusbr0", "uplink"]);
  expect(await client.networks.list({
    filter: ["type=bridge"],
    allProjects: true,
  })).toEqual([{ name: "incusbr0", type: "bridge" }]);
  expect(await client.networks.get("incusbr0")).toEqual({
    value: {
      name: "incusbr0",
      type: "bridge",
    },
    etag: "network-etag",
  });
  expect(await client.networks.state("incusbr0")).toEqual({
    hwaddr: "00:16:3e:ab:cd:ef",
    mtu: 1500,
    addresses: [{ family: "inet", address: "10.10.10.1" }],
  });
  expect(await client.networks.leases("incusbr0")).toEqual([
    {
      hostname: "vm-a",
      address: "10.10.10.42",
      hwaddr: "00:16:3e:aa:bb:cc",
    },
  ]);
  expect(await client.networks.allocations({ allProjects: true })).toEqual([
    {
      project: "default",
      used_by: "/1.0/instances/vm-a",
      address: "10.10.10.42",
    },
  ]);

  const namesCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "GET" &&
      call.path === "/1.0/networks" &&
      call.options?.query === undefined,
  );
  const listCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "GET" &&
      call.path === "/1.0/networks" &&
      String(call.options?.query?.recursion ?? "") === "1",
  );
  const getCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "GET" &&
      call.path === "/1.0/networks/incusbr0" &&
      call.options?.query === undefined,
  );
  const stateCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "GET" &&
      call.path === "/1.0/networks/incusbr0/state",
  );
  const leasesCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "GET" &&
      call.path === "/1.0/networks/incusbr0/leases",
  );
  const allocationsCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "GET" &&
      call.path === "/1.0/network-allocations",
  );

  expect(namesCall).toBeDefined();
  expect(listCall?.options?.query).toEqual({
    recursion: 1,
    "all-projects": "true",
    filter: "type eq bridge",
  });
  expect(getCall).toBeDefined();
  expect(stateCall).toBeDefined();
  expect(leasesCall).toBeDefined();
  expect(allocationsCall?.options?.query).toEqual({
    "all-projects": "true",
  });
});

test("networks lifecycle methods send expected bodies and etags", async () => {
  const transport = new FakeTransport();

  transport.on("POST", "/1.0/networks", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: null,
    },
    headers: new Headers(),
  }));
  transport.on("PUT", "/1.0/networks/incusbr0", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: null,
    },
    headers: new Headers(),
  }));
  transport.on("POST", "/1.0/networks/incusbr0", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: null,
    },
    headers: new Headers(),
  }));
  transport.on("DELETE", "/1.0/networks/incusbr0", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: null,
    },
    headers: new Headers(),
  }));

  const client = Incus.fromTransport("https://incus.example.internal", transport);

  await client.networks.create({
    name: "incusbr0",
    type: "ovn",
    config: {
      network: "none",
      "ipv4.dhcp.gateway": "10.10.10.1",
      "dns.nameservers": "10.10.10.1",
    },
  });
  await client.networks.update(
    "incusbr0",
    {
      config: {
        "ipv4.dhcp.gateway": "10.10.10.2",
      },
    },
    { etag: "network-etag" },
  );
  await client.networks.rename("incusbr0", { name: "incusbr1" });
  await client.networks.remove("incusbr0");

  const createCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "POST" && call.path === "/1.0/networks",
  );
  const updateCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "PUT" && call.path === "/1.0/networks/incusbr0",
  );
  const renameCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "POST" && call.path === "/1.0/networks/incusbr0",
  );
  const removeCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "DELETE" && call.path === "/1.0/networks/incusbr0",
  );

  expect(createCall?.options?.body).toEqual({
    name: "incusbr0",
    type: "ovn",
    config: {
      network: "none",
      "ipv4.dhcp.gateway": "10.10.10.1",
      "dns.nameservers": "10.10.10.1",
    },
  });
  expect(updateCall?.options?.body).toEqual({
    config: {
      "ipv4.dhcp.gateway": "10.10.10.2",
    },
  });
  expect(updateCall?.options?.etag).toBe("network-etag");
  expect(renameCall?.options?.body).toEqual({ name: "incusbr1" });
  expect(removeCall).toBeDefined();
});

test("network ACL APIs use expected paths, queries, and bodies", async () => {
  const transport = new FakeTransport();

  transport.on("GET", "/1.0/network-acls", (options) => {
    if (String(options?.query?.recursion ?? "") === "1") {
      return {
        status: 200,
        data: {
          type: "sync",
          status: "Success",
          status_code: 200,
          metadata: [{ name: "egress-only", project: "default" }],
        },
        headers: new Headers(),
      };
    }

    return {
      status: 200,
      data: {
        type: "sync",
        status: "Success",
        status_code: 200,
        metadata: ["/1.0/network-acls/egress-only"],
      },
      headers: new Headers(),
    };
  });
  transport.on("GET", "/1.0/network-acls/egress-only", () => ({
    status: 200,
    etag: "acl-etag",
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: { name: "egress-only", description: "Only proxy egress" },
    },
    headers: new Headers(),
  }));
  transport.on("GET", "/1.0/network-acls/egress-only/log", () => ({
    status: 200,
    data: "allow 10.10.10.2\n",
    headers: new Headers(),
  }));
  transport.on("POST", "/1.0/network-acls", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: null,
    },
    headers: new Headers(),
  }));
  transport.on("PUT", "/1.0/network-acls/egress-only", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: null,
    },
    headers: new Headers(),
  }));
  transport.on("POST", "/1.0/network-acls/egress-only", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: null,
    },
    headers: new Headers(),
  }));
  transport.on("DELETE", "/1.0/network-acls/egress-only", () => ({
    status: 200,
    data: {
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: null,
    },
    headers: new Headers(),
  }));

  const client = Incus.fromTransport("https://incus.example.internal", transport);

  expect(await client.networks.acls.names()).toEqual(["egress-only"]);
  expect(await client.networks.acls.list({ allProjects: true })).toEqual([
    { name: "egress-only", project: "default" },
  ]);
  expect(await client.networks.acls.get("egress-only")).toEqual({
    value: {
      name: "egress-only",
      description: "Only proxy egress",
    },
    etag: "acl-etag",
  });
  expect(await new Response(await client.networks.acls.getLog("egress-only")).text()).toBe(
    "allow 10.10.10.2\n",
  );

  await client.networks.acls.create({
    name: "egress-only",
    ingress: [],
    egress: [],
  });
  await client.networks.acls.update(
    "egress-only",
    { description: "Updated ACL" },
    { etag: "acl-etag" },
  );
  await client.networks.acls.rename("egress-only", { name: "proxy-only" });
  await client.networks.acls.remove("egress-only");

  const listCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "GET" &&
      call.path === "/1.0/network-acls" &&
      String(call.options?.query?.recursion ?? "") === "1",
  );
  const createCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "POST" && call.path === "/1.0/network-acls",
  );
  const updateCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "PUT" && call.path === "/1.0/network-acls/egress-only",
  );
  const renameCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "POST" && call.path === "/1.0/network-acls/egress-only",
  );
  const removeCall = transport.calls.find(
    (call) => call.method.toUpperCase() === "DELETE" &&
      call.path === "/1.0/network-acls/egress-only",
  );

  expect(listCall?.options?.query).toEqual({
    recursion: 1,
    "all-projects": "true",
  });
  expect(createCall?.options?.body).toEqual({
    name: "egress-only",
    ingress: [],
    egress: [],
  });
  expect(updateCall?.options?.body).toEqual({ description: "Updated ACL" });
  expect(updateCall?.options?.etag).toBe("acl-etag");
  expect(renameCall?.options?.body).toEqual({ name: "proxy-only" });
  expect(removeCall).toBeDefined();
});

test("connectHttp networks.list uses fetch transport request URLs", async () => {
  const requests: Array<{ method?: string; url: string }> = [];
  const client = await Incus.connectHttp({
    endpoint: "https://incus.example.internal/",
    fetch: async (input, init) => {
      requests.push({
        method: init?.method ?? (input instanceof Request ? input.method : undefined),
        url: input instanceof Request ? input.url : String(input),
      });

      return new Response(JSON.stringify({
        type: "sync",
        status: "Success",
        status_code: 200,
        metadata: [{ name: "incusbr0", type: "bridge" }],
      }), {
        status: 200,
        headers: {
          "content-type": "application/json",
        },
      });
    },
  });

  expect(await client.networks.list({ filter: ["type=bridge"] })).toEqual([
    { name: "incusbr0", type: "bridge" },
  ]);

  expect(requests).toHaveLength(1);
  const requestUrl = new URL(requests[0]!.url);
  expect(requests[0]?.method).toBe("GET");
  expect(requestUrl.pathname).toBe("/1.0/networks");
  expect(requestUrl.searchParams.get("recursion")).toBe("1");
  expect(requestUrl.searchParams.get("filter")).toBe("type eq bridge");
});

test("connectUnix networks.state uses unix socket request paths", async () => {
  const tempRoot = mkdtempSync(join(tmpdir(), "incus-ts-"));
  const socketPath = join(tempRoot, "incus.sock");
  const requests: string[] = [];
  const server = createServer((request, response) => {
    requests.push(request.url ?? "");
    response.statusCode = 200;
    response.setHeader("content-type", "application/json");
    response.end(JSON.stringify({
      type: "sync",
      status: "Success",
      status_code: 200,
      metadata: {
        state: "up",
        type: "broadcast",
      },
    }));
  });

  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => {
      reject(error);
    };

    server.once("error", onError);
    server.listen(socketPath, () => {
      server.off("error", onError);
      resolve();
    });
  });

  try {
    const client = await Incus.connectUnix({ socketPath });
    expect(await client.networks.state("incusbr0")).toEqual({
      state: "up",
      type: "broadcast",
    });
    expect(requests).toEqual(["/1.0/networks/incusbr0/state"]);
  } finally {
    await new Promise<void>((resolve) => {
      server.close(() => {
        resolve();
      });
    });
    rmSync(tempRoot, { recursive: true, force: true });
  }
});
