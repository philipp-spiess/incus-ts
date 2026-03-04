import { expect, test } from "bun:test";

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
}

test("creates a typed Incus client with grouped APIs", async () => {
  const client = await Incus.connect("https://incus.example.internal/");

  expect(client).toBeInstanceOf(IncusClient);
  expect(client.endpoint).toBe("https://incus.example.internal");
  expect(typeof client.instances.list).toBe("function");
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

  const operation = await client.instances.create({ name: "c1" });
  expect(operation.id).toBe("op-123");
  expect(await operation.wait()).toEqual({ done: true });

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
