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
    write({ type: "extension_ui_request", method: "notify", message: "Checked cache" });
    write({ type: "extension_ui_request", method: "notify", message: "Loaded context" });
    write({ type: "tool_execution_start", toolCallId: "tool-1", toolName: "web_search", args: { query: "x" } });
    write({ type: "tool_execution_end", toolCallId: "tool-1", toolName: "web_search", result: { summary: "Found results" } });
    write({ type: "message_update", message: { role: "assistant" }, assistantMessageEvent: { type: "text_delta", delta: "Hello" } });
    write({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "Hello world" }] } });
    write({ type: "agent_end", messages: [{ role: "assistant", content: [{ type: "text", text: "Hello world" }] }] });
`),
    );
    const adapter = yield* makePiAdapter({ enabled: true, binaryPath: fakePi, customModels: [] });
    const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 16)).pipe(Effect.forkChild);
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
    const notificationTaskIds = events
      .filter((event) => event.type === "task.progress" && event.payload.taskType === "notification")
      .map((event) => (event.type === "task.progress" ? String(event.payload.taskId) : ""));
    NodeAssert.deepEqual(notificationTaskIds.length, 2);
    NodeAssert.equal(new Set(notificationTaskIds).size, 2);
    NodeAssert.ok(notificationTaskIds.every((taskId) => taskId.startsWith("pi-notification-")));
    NodeAssert.ok(events.some((event) => event.type === "task.progress" && String(event.payload.lastToolName) === "Web search"));
    NodeAssert.ok(events.some((event) => event.type === "task.completed" && event.payload.status === "completed"));
    NodeAssert.ok(events.some((event) => event.type === "item.started" && event.payload.title === "Web search"));
    NodeAssert.ok(events.some((event) => event.type === "item.completed" && event.payload.detail === "Found results"));
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

it.effect("PiAdapter completes active reasoning trace when interrupted", () =>
  Effect.gen(function* () {
    const fakePi = yield* Effect.promise(() =>
      writeFakePiScript(`
    response(msg.id, msg.type, {});
`),
    );
    const adapter = yield* makePiAdapter({ enabled: true, binaryPath: fakePi, customModels: [] });
    const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 9)).pipe(Effect.forkChild);
    const threadId = asThreadId("pi-adapter-interrupt-thread");
    yield* adapter.startSession({
      threadId,
      provider: ProviderDriverKind.make("pi"),
      cwd: tmpdir(),
      runtimeMode: "full-access",
      modelSelection: createModelSelection(ProviderInstanceId.make("pi"), "openai/gpt-5.5"),
    });
    const turn = yield* adapter.sendTurn({ threadId, input: "test" });
    yield* adapter.interruptTurn(threadId, turn.turnId);
    const events = Array.from(yield* Fiber.join(eventsFiber));
    NodeAssert.ok(events.some((event) => event.type === "task.completed" && event.payload.status === "completed"));
    NodeAssert.ok(events.some((event) => event.type === "turn.completed" && event.payload.state === "cancelled"));
    const sessions = yield* adapter.listSessions();
    NodeAssert.equal(sessions[0]?.status, "ready");
  }),
);

it.effect("PiAdapter fails and clears active turn when the Pi process exits mid-turn", () =>
  Effect.gen(function* () {
    const fakePi = yield* Effect.promise(() =>
      writeFakePiScript(`
    process.exit(2);
`),
    );
    const adapter = yield* makePiAdapter({ enabled: true, binaryPath: fakePi, customModels: [] });
    const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 8)).pipe(Effect.forkChild);
    const threadId = asThreadId("pi-adapter-exit-thread");
    yield* adapter.startSession({
      threadId,
      provider: ProviderDriverKind.make("pi"),
      cwd: tmpdir(),
      runtimeMode: "full-access",
      modelSelection: createModelSelection(ProviderInstanceId.make("pi"), "openai/gpt-5.5"),
    });
    yield* Effect.exit(adapter.sendTurn({ threadId, input: "test" }));
    const events = Array.from(yield* Fiber.join(eventsFiber));
    NodeAssert.ok(events.some((event) => event.type === "task.completed" && event.payload.status === "completed"));
    NodeAssert.ok(events.some((event) => event.type === "turn.completed" && event.payload.state === "failed"));
    const sessions = yield* adapter.listSessions();
    NodeAssert.equal(sessions[0]?.status, "error");
    NodeAssert.equal(sessions[0]?.activeTurnId, undefined);
  }),
);

it.effect("PiAdapter ignores non-assistant message_update text", () =>
  Effect.gen(function* () {
    const fakePi = yield* Effect.promise(() =>
      writeFakePiScript(`
    write({ type: "message_update", message: { role: "user" }, assistantMessageEvent: { type: "text_delta", delta: "Do not show" } });
    write({ type: "message_update", message: { role: "toolResult" }, assistantMessageEvent: { type: "thinking_delta", delta: "Do not think" } });
    write({ type: "agent_end", messages: [{ role: "assistant", content: [{ type: "text", text: "Visible answer" }] }] });
`),
    );
    const adapter = yield* makePiAdapter({ enabled: true, binaryPath: fakePi, customModels: [] });
    const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 9)).pipe(Effect.forkChild);
    const threadId = asThreadId("pi-adapter-non-assistant-update-thread");
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
    NodeAssert.deepEqual(assistantDeltas, ["Visible answer"]);
    NodeAssert.equal(
      events.some(
        (event) =>
          event.type === "content.delta" &&
          (event.payload.delta === "Do not show" || event.payload.delta === "Do not think"),
      ),
      false,
    );
  }),
);

it.effect("PiAdapter surfaces canonical final text when it revises a streamed draft", () =>
  Effect.gen(function* () {
    const fakePi = yield* Effect.promise(() =>
      writeFakePiScript(`
    write({ type: "message_update", message: { role: "assistant" }, assistantMessageEvent: { type: "text_delta", delta: "Hello world!!!" } });
    write({ type: "agent_end", messages: [{ role: "assistant", content: [{ type: "text", text: "Hello world" }] }] });
`),
    );
    const adapter = yield* makePiAdapter({ enabled: true, binaryPath: fakePi, customModels: [] });
    const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 9)).pipe(Effect.forkChild);
    const threadId = asThreadId("pi-adapter-final-revision-thread");
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
    NodeAssert.deepEqual(assistantDeltas, ["Hello world!!!", "\n\nHello world"]);
  }),
);

it.effect("PiAdapter surfaces canonical final text when streamed text is a suffix", () =>
  Effect.gen(function* () {
    const fakePi = yield* Effect.promise(() =>
      writeFakePiScript(`
    write({ type: "message_update", message: { role: "assistant" }, assistantMessageEvent: { type: "text_delta", delta: "world" } });
    write({ type: "agent_end", messages: [{ role: "assistant", content: [{ type: "text", text: "Hello world!" }] }] });
`),
    );
    const adapter = yield* makePiAdapter({ enabled: true, binaryPath: fakePi, customModels: [] });
    const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 9)).pipe(Effect.forkChild);
    const threadId = asThreadId("pi-adapter-overlap-thread");
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
    NodeAssert.deepEqual(assistantDeltas, ["world", "\n\nHello world!"]);
  }),
);

it.effect("PiAdapter reconciles last text-bearing agent_end message", () =>
  Effect.gen(function* () {
    const fakePi = yield* Effect.promise(() =>
      writeFakePiScript(`
    write({ type: "agent_end", messages: [
      { role: "assistant", content: [{ type: "text", text: "Visible answer" }] },
      { role: "custom", content: [] }
    ] });
`),
    );
    const adapter = yield* makePiAdapter({ enabled: true, binaryPath: fakePi, customModels: [] });
    const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 9)).pipe(Effect.forkChild);
    const threadId = asThreadId("pi-adapter-agent-end-text-bearing-thread");
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
    NodeAssert.deepEqual(assistantDeltas, ["Visible answer"]);
    NodeAssert.ok(events.some((event) => event.type === "turn.completed" && event.payload.state === "completed"));
  }),
);

it.effect("PiAdapter reconciles custom agent_end text as assistant output", () =>
  Effect.gen(function* () {
    const fakePi = yield* Effect.promise(() =>
      writeFakePiScript(`
    write({ type: "agent_end", messages: [{ role: "custom", content: [{ type: "text", text: "Custom final answer" }] }] });
`),
    );
    const adapter = yield* makePiAdapter({ enabled: true, binaryPath: fakePi, customModels: [] });
    const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 9)).pipe(Effect.forkChild);
    const threadId = asThreadId("pi-adapter-custom-agent-end-thread");
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
    NodeAssert.deepEqual(assistantDeltas, ["Custom final answer"]);
    NodeAssert.ok(events.some((event) => event.type === "turn.completed" && event.payload.state === "completed"));
  }),
);

it.effect("PiAdapter completes turn from custom message_end text", () =>
  Effect.gen(function* () {
    const fakePi = yield* Effect.promise(() =>
      writeFakePiScript(`
    write({ type: "message_update", message: { role: "assistant" }, assistantMessageEvent: { type: "text_delta", delta: "Partial" } });
    write({ type: "message_end", message: { role: "custom", content: [{ type: "text", text: "Partial final answer" }] } });
`),
    );
    const adapter = yield* makePiAdapter({ enabled: true, binaryPath: fakePi, customModels: [] });
    const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 10)).pipe(Effect.forkChild);
    const threadId = asThreadId("pi-adapter-custom-message-thread");
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
    NodeAssert.deepEqual(assistantDeltas, ["Partial", " final answer"]);
    NodeAssert.ok(events.some((event) => event.type === "turn.completed" && event.payload.state === "completed"));
    const sessions = yield* adapter.listSessions();
    NodeAssert.equal(sessions[0]?.status, "ready");
    NodeAssert.equal(sessions[0]?.activeTurnId, undefined);
  }),
);

it.effect("PiAdapter fails and clears active turn when prompt RPC fails", () =>
  Effect.gen(function* () {
    const fakePi = yield* Effect.promise(() =>
      writeFakePiScript(`
    write({ type: "response", id: msg.id, command: msg.type, success: false, error: "prompt failed" });
`),
    );
    const adapter = yield* makePiAdapter({ enabled: true, binaryPath: fakePi, customModels: [] });
    const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 8)).pipe(Effect.forkChild);
    const threadId = asThreadId("pi-adapter-prompt-fail-thread");
    yield* adapter.startSession({
      threadId,
      provider: ProviderDriverKind.make("pi"),
      cwd: tmpdir(),
      runtimeMode: "full-access",
      modelSelection: createModelSelection(ProviderInstanceId.make("pi"), "openai/gpt-5.5"),
    });
    const exit = yield* Effect.exit(adapter.sendTurn({ threadId, input: "test" }));
    NodeAssert.equal(exit._tag, "Failure");
    const events = Array.from(yield* Fiber.join(eventsFiber));
    NodeAssert.ok(events.some((event) => event.type === "task.completed" && event.payload.status === "completed"));
    NodeAssert.ok(events.some((event) => event.type === "turn.completed" && event.payload.state === "failed"));
    const sessions = yield* adapter.listSessions();
    NodeAssert.equal(sessions[0]?.status, "error");
    NodeAssert.equal(sessions[0]?.activeTurnId, undefined);
  }),
);
