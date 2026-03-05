import { expect, test } from "bun:test";
import { EventEmitter } from "node:events";

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

  on(type: string, listener: (...args: unknown[]) => void) {
    this.events.on(type, listener);
  }

  off(type: string, listener: (...args: unknown[]) => void) {
    this.events.off(type, listener);
  }

  send(_data: unknown) {
    // No-op for tests.
  }

  close() {
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
