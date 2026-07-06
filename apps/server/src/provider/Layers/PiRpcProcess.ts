import * as NodeChildProcess from "node:child_process";
import * as NodeReadline from "node:readline";
import * as Effect from "effect/Effect";

export interface PiRpcProcessOptions {
  readonly binaryPath: string;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv | undefined;
  readonly args?: ReadonlyArray<string> | undefined;
}

export interface PiRpcEventListener {
  (event: unknown): void;
}

export interface PiRpcClient {
  readonly send: (command: Record<string, unknown>) => Promise<unknown>;
  readonly notify: (command: Record<string, unknown>) => void;
  readonly stop: () => Promise<void>;
  readonly getStderr: () => string;
}

interface PendingRequest {
  readonly resolve: (value: unknown) => void;
  readonly reject: (cause: Error) => void;
  readonly timeout: NodeJS.Timeout;
}

function isResponseFor(
  data: unknown,
  id: string,
): data is { success: boolean; data?: unknown; error?: string } {
  return (
    !!data &&
    typeof data === "object" &&
    (data as { type?: unknown }).type === "response" &&
    (data as { id?: unknown }).id === id
  );
}

function responseData(response: unknown): unknown {
  if (!response || typeof response !== "object") return undefined;
  const record = response as { success?: unknown; data?: unknown; error?: unknown };
  if (record.success === true) return record.data;
  throw new Error(typeof record.error === "string" ? record.error : "Pi RPC request failed.");
}

export function startPiRpcProcess(
  options: PiRpcProcessOptions,
  onEvent: PiRpcEventListener,
): Promise<PiRpcClient> {
  return new Promise((resolve, reject) => {
    let requestId = 0;
    let stderr = "";
    const pending = new Map<string, PendingRequest>();
    let settled = false;
    const child = NodeChildProcess.spawn(
      options.binaryPath,
      ["--mode", "rpc", ...(options.args ?? [])],
      {
        cwd: options.cwd,
        env: { ...process.env, ...options.environment },
        stdio: "pipe",
      },
    ) as NodeChildProcess.ChildProcessWithoutNullStreams;

    const failAll = (cause: Error) => {
      for (const [id, request] of pending) {
        clearTimeout(request.timeout);
        request.reject(cause);
        pending.delete(id);
      }
    };

    child.stderr.on("data", (chunk) => {
      stderr += Buffer.from(chunk).toString("utf8");
    });
    child.once("error", (cause) => {
      const error = new Error(`Failed to start Pi RPC process: ${cause.message}`);
      if (!settled) {
        settled = true;
        reject(error);
      }
      failAll(error);
    });
    child.once("exit", (code, signal) => {
      const message = `Pi RPC process exited (code=${code} signal=${signal}). ${stderr}`.trim();
      const error = new Error(message);
      onEvent({ type: "process_exit", error: message, code, signal });
      if (!settled) {
        settled = true;
        reject(error);
      }
      failAll(error);
    });

    NodeReadline.createInterface({ input: child.stdout }).on("line", (line) => {
      let data: unknown;
      try {
        data = JSON.parse(line);
      } catch {
        return;
      }
      if (data && typeof data === "object") {
        const id = (data as { id?: unknown }).id;
        if (typeof id === "string" && isResponseFor(data, id)) {
          const request = pending.get(id);
          if (request) {
            pending.delete(id);
            clearTimeout(request.timeout);
            try {
              request.resolve(responseData(data));
            } catch (cause) {
              request.reject(cause instanceof Error ? cause : new Error(String(cause)));
            }
            return;
          }
        }
      }
      onEvent(data);
    });

    const client: PiRpcClient = {
      send: (command) =>
        new Promise((requestResolve, requestReject) => {
          if (child.exitCode !== null || !child.stdin.writable) {
            requestReject(new Error(`Pi RPC process is not running. ${stderr}`.trim()));
            return;
          }
          const id = `t3_${++requestId}`;
          const timeout = setTimeout(() => {
            pending.delete(id);
            requestReject(
              new Error(`Timed out waiting for Pi RPC response to ${String(command.type)}.`),
            );
          }, 30_000);
          pending.set(id, {
            resolve: requestResolve,
            reject: requestReject,
            timeout,
          });
          child.stdin.write(`${JSON.stringify({ ...command, id })}\n`);
        }),
      notify: (command) => {
        if (child.exitCode !== null || !child.stdin.writable) {
          throw new Error(`Pi RPC process is not running. ${stderr}`.trim());
        }
        child.stdin.write(`${JSON.stringify(command)}\n`);
      },
      stop: () =>
        new Promise((stopResolve) => {
          if (child.exitCode !== null) {
            stopResolve();
            return;
          }
          const timer = setTimeout(() => {
            child.kill("SIGKILL");
            stopResolve();
          }, 1_000);
          child.once("exit", () => {
            clearTimeout(timer);
            stopResolve();
          });
          child.kill("SIGTERM");
        }),
      getStderr: () => stderr,
    };

    setTimeout(() => {
      if (!settled) {
        settled = true;
        resolve(client);
      }
    }, 100);
  });
}

export function probePiAvailableModels(
  options: PiRpcProcessOptions,
): Effect.Effect<ReadonlyArray<unknown>, Error> {
  return Effect.tryPromise({
    try: async () => {
      const client = await startPiRpcProcess(options, () => undefined);
      try {
        const data = await client.send({ type: "get_available_models" });
        if (
          data &&
          typeof data === "object" &&
          Array.isArray((data as { models?: unknown }).models)
        ) {
          return (data as { models: unknown[] }).models;
        }
        return [];
      } finally {
        await client.stop();
      }
    },
    catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
  });
}

export function probePiSlashCommands(
  options: PiRpcProcessOptions,
): Effect.Effect<ReadonlyArray<unknown>, Error> {
  return Effect.tryPromise({
    try: async () => {
      const client = await startPiRpcProcess(options, () => undefined);
      try {
        const data = await client.send({ type: "get_commands" });
        if (
          data &&
          typeof data === "object" &&
          Array.isArray((data as { commands?: unknown }).commands)
        ) {
          return (data as { commands: unknown[] }).commands;
        }
        return [];
      } finally {
        await client.stop();
      }
    },
    catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
  });
}
