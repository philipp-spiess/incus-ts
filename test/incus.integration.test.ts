import { expect, test } from "bun:test";
import { existsSync } from "node:fs";

import { Incus, IncusApiError, type IncusClient, type IncusRecord } from "../src/index";

const textDecoder = new TextDecoder();
const runE2E = process.env.INCUS_E2E === "1";
const integrationTest = runE2E ? test : test.skip;
const traceE2E = process.env.INCUS_E2E_TRACE === "1";

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

type TraceMark = {
  label: string;
  startMs: number;
};

type TraceSpan = {
  label: string;
  durationMs: number;
};

function decodeBytes(bytes?: Uint8Array): string {
  if (!bytes || bytes.byteLength === 0) {
    return "";
  }

  return textDecoder.decode(bytes);
}

function traceStart(label: string): TraceMark {
  return {
    label,
    startMs: performance.now(),
  };
}

function traceEnd(spans: TraceSpan[], mark: TraceMark): void {
  spans.push({
    label: mark.label,
    durationMs: performance.now() - mark.startMs,
  });
}

function printTrace(spans: TraceSpan[]): void {
  if (!traceE2E) {
    return;
  }

  const phaseSpans = spans.filter((span) => span.label !== "total");
  const rounded = phaseSpans.map((span) => ({
    label: span.label,
    ms: Math.round(span.durationMs),
  }));
  const total = rounded.reduce((sum, span) => sum + span.ms, 0);

  console.log("[incus-e2e-trace] phase timings (ms):");
  for (const span of rounded) {
    console.log(`[incus-e2e-trace] - ${span.label}: ${span.ms}`);
  }
  console.log(`[incus-e2e-trace] - total: ${total}`);
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

function makeCaptureSink(
  chunks: Uint8Array[],
  onChunk?: () => void,
): {
  stream: WritableStream<Uint8Array>;
  done: Promise<void>;
} {
  let resolveDone: (() => void) | undefined;
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve;
  });

  return {
    stream: new WritableStream<Uint8Array>({
      write(chunk) {
        chunks.push(chunk.slice());
        onChunk?.();
      },
      close() {
        resolveDone?.();
      },
      abort() {
        resolveDone?.();
      },
    }),
    done,
  };
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
    await client.instances.instance(name).get();
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
      const createOperation = client.instances.create({
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
  const stdoutCapture = makeCaptureSink(stdoutChunks);
  const stderrCapture = makeCaptureSink(stderrChunks);

  const operation = client.instances.instance(instanceName).exec(
    {
      command,
      interactive: false,
    },
    {
      stdout: stdoutCapture.stream,
      stderr: stderrCapture.stream,
    },
  );

  const result = await operation.wait({ timeoutSeconds });
  await withTimeout(
    Promise.all([stdoutCapture.done, stderrCapture.done]).then(() => {}),
    5_000,
    "exec output stream flush",
  );
  return {
    stdout: concatChunks(stdoutChunks),
    stderr: concatChunks(stderrChunks),
    operation: result,
  };
}

integrationTest(
  "e2e: creates instance, validates streaming exec output, makes network call, and cleans up",
  async () => {
    const traceSpans: TraceSpan[] = [];
    const totalTrace = traceStart("total");

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
      const createTrace = traceStart("createContainer");
      const imageLabel = await createContainer(client, instanceName);
      traceEnd(traceSpans, createTrace);
      created = true;
      expect(imageLabel.length).toBeGreaterThan(0);

      const startTrace = traceStart("startContainer");
      const startOperation = client.instances.instance(instanceName).setState({
        action: "start",
        timeout: 180,
      });
      await startOperation.wait({ timeoutSeconds: 240 });
      traceEnd(traceSpans, startTrace);

      const streamingChunks: Uint8Array[] = [];
      let firstChunkSeen = false;
      let resolveFirstChunk: (() => void) | undefined;
      const firstChunkPromise = new Promise<void>((resolve) => {
        resolveFirstChunk = resolve;
      });
      let stdinController: ReadableStreamDefaultController<Uint8Array> | undefined;
      const gatedStdin = new ReadableStream<Uint8Array>({
        start(controller) {
          stdinController = controller;
        },
      });

      const streamStartTrace = traceStart("streamExec.startAndAttach");
      const streamingOperation = client.instances.instance(instanceName).exec(
        {
          command: ["sh", "-lc", "echo stream:1; cat >/dev/null; echo stream:2"],
          interactive: false,
        },
        {
          stdin: gatedStdin,
          stdout: "pipe",
          stderr: "pipe",
        },
      );
      traceEnd(traceSpans, streamStartTrace);

      const consumeStreamTrace = traceStart("streamExec.consumeStdout");
      const stdoutConsume = (async () => {
        for await (const chunk of streamingOperation) {
          streamingChunks.push(chunk);
          if (!firstChunkSeen) {
            firstChunkSeen = true;
            resolveFirstChunk?.();
          }
        }
      })();

      let operationDone = false;
      const waitForStreamingCommand = streamingOperation.waitResult({ timeoutSeconds: 180 }).then((op) => {
        operationDone = true;
        return op;
      });

      const firstChunkTrace = traceStart("streamExec.waitFirstChunk");
      await withTimeout(firstChunkPromise, 15_000, "the first streamed chunk");
      traceEnd(traceSpans, firstChunkTrace);
      expect(operationDone).toBe(false);
      stdinController?.close();

      const waitDoneTrace = traceStart("streamExec.waitDone");
      const streamingResult = await withTimeout(
        waitForStreamingCommand,
        180_000,
        "the streamed command completion",
      );
      traceEnd(traceSpans, waitDoneTrace);
      await withTimeout(stdoutConsume, 5_000, "stdout stream consume");
      traceEnd(traceSpans, consumeStreamTrace);
      expect(streamingResult.ok).toBe(true);

      const streamedText = concatChunks(streamingChunks);
      expect(streamedText).toContain("stream:1");
      expect(streamedText).toContain("stream:2");

      const networkTrace = traceStart("networkExec");
      const network = await execAndCapture(
        client,
        instanceName,
        [
          "sh",
          "-lc",
          "udhcpc -i eth0 -q -n >/dev/null 2>&1 || true; "
            + "GW=$(ip route | awk '/default/ {print $3; exit}'); "
            + "ping -c 1 -W 2 \"${GW:-192.168.100.1}\" >/tmp/ping.out 2>&1; "
            + "rc=$?; cat /tmp/ping.out; "
            + "if [ \"$rc\" -eq 0 ]; then echo __PING_OK__; fi; "
            + "exit \"$rc\"",
        ],
        180,
      );
      traceEnd(traceSpans, networkTrace);
      const networkOutput = `${network.stdout}\n${network.stderr}`;
      expect(networkOutput).toContain("__PING_OK__");

      const instance = client.instances.instance(instanceName);
      const snapshotName = `snap-${Date.now().toString(36)}`;
      const snapshotFile = "/root/incus-ts-snapshot-state.txt";
      const beforeValue = `before-${Date.now().toString(36)}`;
      const afterValue = `after-${Date.now().toString(36)}`;

      const snapshotWriteBeforeTrace = traceStart("snapshot.writeBefore");
      const writeBefore = await instance.exec(
        {
          command: ["sh", "-lc", `printf '%s' '${beforeValue}' > ${snapshotFile}`],
          interactive: false,
        },
      ).waitResult({ timeoutSeconds: 120 });
      traceEnd(traceSpans, snapshotWriteBeforeTrace);
      expect(writeBefore.ok).toBe(true);

      const snapshotCreateTrace = traceStart("snapshot.create");
      const snapshotCreate = instance.snapshots.create({ name: snapshotName });
      await snapshotCreate.wait({ timeoutSeconds: 180 });
      traceEnd(traceSpans, snapshotCreateTrace);

      const snapshotWriteAfterTrace = traceStart("snapshot.writeAfter");
      const writeAfter = await instance.exec(
        {
          command: ["sh", "-lc", `printf '%s' '${afterValue}' > ${snapshotFile}`],
          interactive: false,
        },
      ).waitResult({ timeoutSeconds: 120 });
      traceEnd(traceSpans, snapshotWriteAfterTrace);
      expect(writeAfter.ok).toBe(true);

      const stopForRestoreTrace = traceStart("snapshot.stopForRestore");
      const stopForRestore = instance.setState({
        action: "stop",
        timeout: 120,
        force: true,
      });
      await stopForRestore.wait({ timeoutSeconds: 180 });
      traceEnd(traceSpans, stopForRestoreTrace);

      const restoreTrace = traceStart("snapshot.restore");
      const restoreOperation = instance.restore(snapshotName);
      await restoreOperation.wait({ timeoutSeconds: 180 });
      traceEnd(traceSpans, restoreTrace);

      const restartAfterRestoreTrace = traceStart("snapshot.restartAfterRestore");
      const restartAfterRestore = instance.setState({
        action: "start",
        timeout: 120,
      });
      await restartAfterRestore.wait({ timeoutSeconds: 180 });
      traceEnd(traceSpans, restartAfterRestoreTrace);

      const snapshotVerifyTrace = traceStart("snapshot.verify");
      const restoredState = await execAndCapture(
        client,
        instanceName,
        ["sh", "-lc", `cat ${snapshotFile}`],
        60,
      );
      traceEnd(traceSpans, snapshotVerifyTrace);
      expect(restoredState.stdout).toContain(beforeValue);
      expect(restoredState.stdout).not.toContain(afterValue);
    } finally {
      const cleanupTrace = traceStart("cleanup.forceDelete");
      if (created) {
        await forceDeleteInstance(client, instanceName);
      }
      traceEnd(traceSpans, cleanupTrace);

      client.disconnect();

      traceEnd(traceSpans, totalTrace);
      printTrace(traceSpans);
    }
  },
  20 * 60_000,
);
