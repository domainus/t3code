import * as NodeAssert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";

import { ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { makePiAdapter } from "./PiAdapter.ts";

const asThreadId = (value: string): ThreadId => ThreadId.make(value);

async function writeFakePiScript(events: string): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "t3-pi-adapter-test-"));
  const script = join(dir, "fake-pi.mjs");
  await writeFile(
    script,
    `#!/usr/bin/env node
import readline from "node:readline";
const rl = readline.createInterface({ input: process.stdin });
function write(value) { process.stdout.write(JSON.stringify(value) + "\\n"); }
function response(id, command, data = {}) { write({ type: "response", id, command, success: true, data }); }
for (const arg of process.argv.slice(2)) {
  if (arg === "--version") { console.log("pi 0.0.0-test"); process.exit(0); }
}
rl.on("line", async (line) => {
  const msg = JSON.parse(line);
  if (msg.type === "get_state") response(msg.id, msg.type, { sessionFile: "/tmp/fake-pi-session.jsonl" });
  else if (msg.type === "get_session_stats") response(msg.id, msg.type, { tokens: { input: 1, output: 2 }, contextUsage: { tokens: 3, contextWindow: 100 } });
  else if (msg.type === "prompt") {
    write({ type: "turn_start" });
${events}
    response(msg.id, msg.type, {});
  } else if (msg.type === "abort") response(msg.id, msg.type, {});
  else response(msg.id, msg.type, {});
});
`,
    { mode: 0o755 },
  );
  return script;
}

it.effect("PiAdapter reconciles final assistant text and surfaces thinking/tool progress", () =>
  Effect.gen(function* () {
    const fakePi = yield* Effect.promise(() =>
      writeFakePiScript(`
    write({ type: "message_update", message: { role: "assistant" }, assistantMessageEvent: { type: "thinking_delta", delta: "Thinking about it" } });
    write({ type: "tool_execution_start", toolCallId: "tool-1", toolName: "web_search", args: { query: "x" } });
    write({ type: "message_update", message: { role: "assistant" }, assistantMessageEvent: { type: "text_delta", delta: "Hello" } });
    write({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "Hello world" }] } });
    write({ type: "agent_end", messages: [{ role: "assistant", content: [{ type: "text", text: "Hello world" }] }] });
`),
    );
    const adapter = yield* makePiAdapter({ enabled: true, binaryPath: fakePi, customModels: [] });
    const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 12)).pipe(Effect.forkChild);
    const threadId = asThreadId("pi-adapter-test-thread");
    yield* adapter.startSession({
      threadId,
      provider: ProviderDriverKind.make("pi"),
      cwd: tmpdir(),
      runtimeMode: "full-access",
      modelSelection: createModelSelection(ProviderInstanceId.make("pi"), "openai/gpt-5.5"),
    });
    yield* adapter.sendTurn({ threadId, input: "test" });
    const events = Array.from(yield* Fiber.join(eventsFiber));
    const assistantDeltas = events
      .filter((event) => event.type === "content.delta" && event.payload.streamKind === "assistant_text")
      .map((event) => (event.type === "content.delta" ? event.payload.delta : ""));
    NodeAssert.deepEqual(assistantDeltas, ["Hello", " world"]);
    NodeAssert.ok(events.some((event) => event.type === "task.progress" && String(event.payload.summary).includes("Thinking")));
    NodeAssert.ok(events.some((event) => event.type === "task.progress" && String(event.payload.lastToolName) === "web_search"));
  }),
);

it.effect("PiAdapter does not turn noninteractive UI notifications into user input requests", () =>
  Effect.gen(function* () {
    const fakePi = yield* Effect.promise(() =>
      writeFakePiScript(`
    write({ type: "extension_ui_request", id: "notify-1", method: "notify", message: "Pi note" });
    write({ type: "extension_ui_request", id: "unknown-1", method: "toast", message: "Pi toast" });
    write({ type: "message_update", message: { role: "assistant" }, assistantMessageEvent: { type: "text_delta", delta: "Done" } });
    write({ type: "agent_end", messages: [{ role: "assistant", content: [{ type: "text", text: "Done" }] }] });
`),
    );
    const adapter = yield* makePiAdapter({ enabled: true, binaryPath: fakePi, customModels: [] });
    const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 10)).pipe(Effect.forkChild);
    const threadId = asThreadId("pi-adapter-notify-thread");
    yield* adapter.startSession({
      threadId,
      provider: ProviderDriverKind.make("pi"),
      cwd: tmpdir(),
      runtimeMode: "full-access",
      modelSelection: createModelSelection(ProviderInstanceId.make("pi"), "openai/gpt-5.5"),
    });
    yield* adapter.sendTurn({ threadId, input: "test" });
    const events = Array.from(yield* Fiber.join(eventsFiber));
    NodeAssert.equal(events.some((event) => event.type === "user-input.requested"), false);
    NodeAssert.ok(events.some((event) => event.type === "task.progress" && String(event.payload.summary).includes("Pi note")));
    NodeAssert.ok(events.some((event) => event.type === "task.progress" && String(event.payload.summary).includes("Pi toast")));
  }),
);
