import { expect, test } from "bun:test";
import { existsSync } from "node:fs";

import { Incus, IncusApiError, type IncusClient, type IncusRecord } from "../src/index";

const textDecoder = new TextDecoder();
const runE2E = process.env.INCUS_E2E === "1";
const integrationTest = runE2E ? test : test.skip;

type ExecCapture = {
  stdout: string;
  stderr: string;
  operation: IncusRecord;
};

type IncusCommandResult = {
  ok: boolean;
  stdout: string;
  stderr: string;
};

function decodeBytes(bytes?: Uint8Array): string {
  if (!bytes || bytes.byteLength === 0) {
    return "";
  }

  return textDecoder.decode(bytes);
}

function runIncusCommand(args: string[]): IncusCommandResult {
  const proc = Bun.spawnSync({
    cmd: ["incus", ...args],
    stdout: "pipe",
    stderr: "pipe",
  });

  return {
    ok: proc.exitCode === 0,
    stdout: decodeBytes(proc.stdout).trim(),
    stderr: decodeBytes(proc.stderr).trim(),
  };
}

function detectIncusSocketPath(): string | null {
  const override = process.env.INCUS_SOCKET_PATH;
  if (override) {
    return override;
  }

  const currentRemote = runIncusCommand(["remote", "get-default"]);
  if (!currentRemote.ok || !currentRemote.stdout) {
    return null;
  }

  const remotes = runIncusCommand(["remote", "list", "--format=json"]);
  if (!remotes.ok || !remotes.stdout) {
    return null;
  }

  try {
    const parsed = JSON.parse(remotes.stdout) as Record<string, { Addr?: string }>;
    const addr = parsed[currentRemote.stdout]?.Addr;
    if (!addr || !addr.startsWith("unix://")) {
      return null;
    }

    const path = addr.slice("unix://".length);
    if (path.length > 0) {
      return path;
    }

    return "/var/lib/incus/unix.socket";
  } catch {
    return null;
  }
}

function concatChunks(chunks: Uint8Array[]): string {
  let total = 0;
  for (const chunk of chunks) {
    total += chunk.byteLength;
  }

  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return textDecoder.decode(out);
}

function makeCaptureStream(
  chunks: Uint8Array[],
  onChunk?: () => void,
): WritableStream<Uint8Array> {
  return new WritableStream<Uint8Array>({
    write(chunk) {
      chunks.push(chunk.slice());
      onChunk?.();
    },
  });
}

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`[incus e2e] Timed out waiting for ${label} after ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function instanceExists(client: IncusClient, name: string): Promise<boolean> {
  try {
    await client.instances.get(name);
    return true;
  } catch (error) {
    if (error instanceof IncusApiError && error.status === 404) {
      return false;
    }

    throw error;
  }
}

async function forceDeleteInstance(client: IncusClient, name: string): Promise<void> {
  const cliDelete = runIncusCommand(["delete", name, "--force"]);
  for (let attempt = 0; attempt < 25; attempt += 1) {
    if (!(await instanceExists(client, name))) {
      return;
    }

    await sleep(100);
  }

  const cliError = cliDelete.stderr || cliDelete.stdout || "unknown CLI deletion error";
  throw new Error(`[incus e2e] Failed to delete instance ${name}. CLI: ${cliError}`);
}

async function createContainer(client: IncusClient, name: string): Promise<string> {
  const preferredAlias = process.env.INCUS_TEST_IMAGE_ALIAS;
  const fallbackAlias = process.env.INCUS_TEST_FALLBACK_IMAGE_ALIAS ?? "alpine/3.20";
  const fallbackServer = process.env.INCUS_TEST_FALLBACK_IMAGE_SERVER
    ?? "https://images.linuxcontainers.org";
  const fallbackProtocol = process.env.INCUS_TEST_FALLBACK_IMAGE_PROTOCOL ?? "simplestreams";

  const candidates: Array<{ label: string; source: IncusRecord }> = [];
  if (preferredAlias && preferredAlias.length > 0) {
    candidates.push({
      label: preferredAlias,
      source: {
        type: "image",
        alias: preferredAlias,
      },
    });
  }

  candidates.push({
      label: `${fallbackServer}#${fallbackAlias}`,
      source: {
        type: "image",
        mode: "pull",
        server: fallbackServer,
        protocol: fallbackProtocol,
        alias: fallbackAlias,
      },
  });

  let lastError: unknown;
  for (const candidate of candidates) {
    try {
      const createOperation = await client.instances.create({
        name,
        type: "container",
        source: candidate.source,
      });
      await createOperation.wait({ timeoutSeconds: 1800 });
      return candidate.label;
    } catch (error) {
      lastError = error;
      await forceDeleteInstance(client, name);
    }
  }

  if (lastError instanceof IncusApiError) {
    throw new Error(
      `[incus e2e] Failed to create instance using preferred image aliases: ${lastError.message}`,
    );
  }

  throw new Error("[incus e2e] Failed to create instance from all candidate image sources");
}

async function execAndCapture(
  client: IncusClient,
  instanceName: string,
  command: string[],
  timeoutSeconds: number,
): Promise<ExecCapture> {
  const stdoutChunks: Uint8Array[] = [];
  const stderrChunks: Uint8Array[] = [];
  const stdout = makeCaptureStream(stdoutChunks);
  const stderr = makeCaptureStream(stderrChunks);

  const operation = await client.instances.exec(
    instanceName,
    {
      command,
      interactive: false,
    },
    {
      stdout,
      stderr,
    },
  );

  const result = await operation.wait({ timeoutSeconds });
  return {
    stdout: concatChunks(stdoutChunks),
    stderr: concatChunks(stderrChunks),
    operation: result,
  };
}

integrationTest(
  "e2e: creates instance, validates streaming exec output, makes network call, and cleans up",
  async () => {
    const socketPath = detectIncusSocketPath();
    if (!socketPath) {
      throw new Error(
        "[incus e2e] Could not detect an active Incus unix socket. Set INCUS_SOCKET_PATH explicitly.",
      );
    }

    if (!existsSync(socketPath)) {
      throw new Error(
        `[incus e2e] Incus socket does not exist at ${socketPath}. Set INCUS_SOCKET_PATH explicitly.`,
      );
    }

    const client = await Incus.connectUnix({ socketPath });
    const instanceName = `incus-ts-e2e-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

    let created = false;
    try {
      const imageLabel = await createContainer(client, instanceName);
      created = true;
      expect(imageLabel.length).toBeGreaterThan(0);

      const startOperation = await client.instances.setState(instanceName, {
        action: "start",
        timeout: 180,
      });
      await startOperation.wait({ timeoutSeconds: 240 });

      const streamingChunks: Uint8Array[] = [];
      let firstChunkSeen = false;
      let resolveFirstChunk: (() => void) | undefined;
      const firstChunkPromise = new Promise<void>((resolve) => {
        resolveFirstChunk = resolve;
      });
      const streamingStdout = makeCaptureStream(streamingChunks, () => {
        if (!firstChunkSeen) {
          firstChunkSeen = true;
          resolveFirstChunk?.();
        }
      });
      const streamingStderr = makeCaptureStream(streamingChunks);

      const streamingOperation = await client.instances.exec(
        instanceName,
        {
          command: ["sh", "-lc", "for i in 1 2; do echo stream:$i; sleep 1; done"],
          interactive: false,
        },
        {
          stdout: streamingStdout,
          stderr: streamingStderr,
        },
      );

      let operationDone = false;
      const waitForStreamingCommand = streamingOperation.wait({ timeoutSeconds: 180 }).then((op) => {
        operationDone = true;
        return op;
      });

      await withTimeout(firstChunkPromise, 15_000, "the first streamed chunk");
      expect(operationDone).toBe(false);

      const streamingResult = await withTimeout(
        waitForStreamingCommand,
        180_000,
        "the streamed command completion",
      );
      expect(typeof streamingResult.status).toBe("string");

      const streamedText = concatChunks(streamingChunks);
      expect(streamedText).toContain("stream:1");
      expect(streamedText).toContain("stream:2");

      const network = await execAndCapture(
        client,
        instanceName,
        [
          "sh",
          "-lc",
          "udhcpc -i eth0 -q -n >/dev/null 2>&1 || true; "
            + "GW=$(ip route | awk '/default/ {print $3; exit}'); "
            + "ping -c 1 -W 2 \"${GW:-192.168.100.1}\"",
        ],
        180,
      );
      const networkOutput = `${network.stdout}\n${network.stderr}`;
      expect(networkOutput).toContain("1 packets transmitted");
      expect(networkOutput).toMatch(/1 packets received|1 received/);
    } finally {
      if (created) {
        await forceDeleteInstance(client, instanceName);
      }

      client.disconnect();
    }
  },
  20 * 60_000,
);
