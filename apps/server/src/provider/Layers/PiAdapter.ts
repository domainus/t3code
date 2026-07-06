import {
  type ChatAttachment,
  EventId,
  ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeItemId,
  RuntimeRequestId,
  RuntimeTaskId,
  type ThreadId,
  TurnId,
  type PiSettings,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { startPiRpcProcess, type PiRpcClient } from "./PiRpcProcess.ts";

const PROVIDER = ProviderDriverKind.make("pi");
const eventId = () => EventId.make(NodeCrypto.randomUUID());
const runtimeItemId = (id: string) => RuntimeItemId.make(id);
const runtimeRequestId = (id: string) => RuntimeRequestId.make(id);
const runtimeTaskId = (id: string) => RuntimeTaskId.make(id);
const T3_PI_SYSTEM_PROMPT =
  "You are running inside T3 Code. Surface interactive extension UI requests through the host UI when available.";
const T3_PI_ENTRY_CAPTURE_MARKER = "T3_PI_ENTRY_CAPTURE";
const T3_PI_COMMAND_RESULT_MARKER = "T3_PI_COMMAND_RESULT";
const T3_PI_CAPTURE_COMMAND = "t3_capture_entries";
const T3_PI_TREE_COMMAND = "t3_tree";
type PiAdapterError =
  | ProviderAdapterRequestError
  | ProviderAdapterSessionNotFoundError
  | ProviderAdapterValidationError;

interface PiSessionContext {
  session: ProviderSession;
  client: PiRpcClient;
  activeTurnId?: TurnId | undefined;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  pendingExtensionRequests: Map<string, { method: string; kind: "request" | "user-input" }>;
  tempFiles: Set<string>;
  capturedUserEntries: PiCapturedEntry[];
  pendingExtensionResults: Map<
    string,
    { resolve: (value: unknown) => void; reject: (cause: Error) => void }
  >;
  assistantTextByTurn: Map<TurnId, string>;
  reasoningSummaryByTurn: Map<TurnId, string>;
  notificationSequence: number;
}

interface PiCapturedEntry {
  readonly id: string;
  readonly parentId: string | null;
  readonly text: string;
}

export interface PiAdapterLiveOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  readonly attachmentsDir?: string;
  readonly stateDir?: string;
}

interface PiImageContent {
  readonly type: "image";
  readonly data: string;
  readonly mimeType: string;
}

function selectedThinkingLevel(
  options: ReadonlyArray<{ readonly id: string; readonly value: string | boolean }> | undefined,
): string | undefined {
  const value = options?.find((option) => option.id === "thinkingLevel")?.value;
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function textFromEvent(
  event: unknown,
): { kind: "assistant_text" | "reasoning_text"; delta: string; index?: number } | undefined {
  if (!event || typeof event !== "object") return undefined;
  const record = event as Record<string, unknown>;
  if (record.type === "message_update") {
    const message =
      record.message && typeof record.message === "object"
        ? (record.message as Record<string, unknown>)
        : null;
    if (message?.role !== "assistant" && message?.role !== "custom") return undefined;
    const nested = record.assistantMessageEvent;
    if (nested && typeof nested === "object") return textFromEvent(nested);
  }
  if (record.type === "text_delta" && typeof record.delta === "string") {
    return {
      kind: "assistant_text",
      delta: record.delta,
      ...(typeof record.contentIndex === "number" ? { index: record.contentIndex } : {}),
    };
  }
  if (record.type === "thinking_delta" && typeof record.delta === "string") {
    return {
      kind: "reasoning_text",
      delta: record.delta,
      ...(typeof record.contentIndex === "number" ? { index: record.contentIndex } : {}),
    };
  }
  return undefined;
}

function readPiTextContent(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .flatMap((part): string[] => {
      if (!part || typeof part !== "object") return [];
      const record = part as Record<string, unknown>;
      if (record.type === "text" && typeof record.text === "string") return [record.text];
      return [];
    })
    .join("\n\n");
}

function piThinkingTaskId(turnId: TurnId | string): RuntimeTaskId {
  return runtimeTaskId(`pi-thinking-${turnId}`);
}

function piNotificationTaskId(
  turnId: TurnId | string | undefined,
  sequence: number,
): RuntimeTaskId {
  return runtimeTaskId(`pi-notification-${turnId ?? "session"}-${sequence}`);
}

function assistantTextFromPiMessage(message: unknown): string | undefined {
  if (!message || typeof message !== "object") return undefined;
  const record = message as Record<string, unknown>;
  // Pi can emit assistant-visible text as either an assistant message or a
  // custom message. Paseo renders custom message_end text as assistant output;
  // doing the same here prevents the response bubble from staying truncated
  // when Pi finalizes generated text through that channel.
  if (record.role !== "assistant" && record.role !== "custom") return undefined;
  const text = readPiTextContent(record.content).trimEnd();
  return text.length > 0 ? text : undefined;
}

function humanizeToolName(name: string): string {
  return name
    .replace(/^functions[._-]/i, "")
    .replace(/[_-]+/g, " ")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^\w/, (char) => char.toUpperCase());
}

function toolTitle(event: Record<string, unknown>): string {
  const rawName = typeof event.toolName === "string" ? event.toolName : "Tool";
  return humanizeToolName(rawName);
}

function summarizeUnknown(value: unknown, maxLength = 500): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (value === undefined || value === null) return undefined;
  try {
    const serialized = JSON.stringify(value);
    return serialized.length > maxLength ? `${serialized.slice(0, maxLength - 1)}…` : serialized;
  } catch {
    return String(value);
  }
}

function toolDetailFromArgs(toolName: string, args: unknown): string | undefined {
  if (typeof args === "string") return args.trim() || undefined;
  if (!args || typeof args !== "object" || Array.isArray(args)) return undefined;
  const record = args as Record<string, unknown>;
  const lowerName = toolName.toLowerCase();
  const candidates = lowerName.includes("web")
    ? [record.query, record.q, record.prompt, record.url]
    : lowerName.includes("bash") || lowerName.includes("command")
      ? [record.command, record.cmd]
      : lowerName.includes("read") || lowerName.includes("edit") || lowerName.includes("write")
        ? [record.path, record.file, record.filePath]
        : [record.query, record.command, record.path, record.url, record.prompt];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim().length > 0) return candidate.trim();
  }
  const keys = Object.keys(record);
  if (keys.length === 0) return undefined;
  return summarizeUnknown(record);
}

function toolDetailFromResult(result: unknown): string | undefined {
  if (typeof result === "string") return result.trim() || undefined;
  if (!result || typeof result !== "object" || Array.isArray(result)) return summarizeUnknown(result);
  const record = result as Record<string, unknown>;
  for (const key of ["summary", "output", "stdout", "stderr", "text", "message", "error"] as const) {
    const value = record[key];
    if (typeof value === "string" && value.trim().length > 0) return value.trim();
  }
  return summarizeUnknown(record);
}

function toToolItemType(
  toolName: string,
): "command_execution" | "file_change" | "web_search" | "dynamic_tool_call" {
  const lower = toolName.toLowerCase();
  if (lower.includes("bash") || lower.includes("command")) return "command_execution";
  if (lower.includes("edit") || lower.includes("write") || lower.includes("patch"))
    return "file_change";
  if (lower.includes("web")) return "web_search";
  return "dynamic_tool_call";
}

function resumeSessionFile(resumeCursor: unknown): string | undefined {
  if (typeof resumeCursor === "string" && resumeCursor.trim().length > 0) return resumeCursor;
  if (!resumeCursor || typeof resumeCursor !== "object") return undefined;
  const record = resumeCursor as Record<string, unknown>;
  for (const key of ["sessionFile", "nativeHandle", "session", "path"]) {
    const value = record[key];
    if (typeof value === "string" && value.trim().length > 0) return value;
  }
  return undefined;
}

function sessionFileFromState(state: unknown): string | undefined {
  if (!state || typeof state !== "object") return undefined;
  const value = (state as Record<string, unknown>).sessionFile;
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function parseSlashCommand(text: string | undefined): { name: string; args?: string } | undefined {
  const trimmed = text?.trim();
  if (!trimmed?.startsWith("/") || trimmed.length <= 1) return undefined;
  const withoutPrefix = trimmed.slice(1);
  const firstWhitespaceIdx = withoutPrefix.search(/\s/);
  const name = (
    firstWhitespaceIdx === -1 ? withoutPrefix : withoutPrefix.slice(0, firstWhitespaceIdx)
  ).toLowerCase();
  if (!name || name.includes("/")) return undefined;
  const args = firstWhitespaceIdx === -1 ? "" : withoutPrefix.slice(firstWhitespaceIdx + 1).trim();
  return args.length > 0 ? { name, args } : { name };
}

function parseAutoCompactMode(value: string | undefined): boolean | "toggle" | "unknown" {
  const normalized = value?.trim().toLowerCase();
  if (!normalized || normalized === "toggle") return "toggle";
  if (["on", "true", "yes", "enable", "enabled"].includes(normalized)) return true;
  if (["off", "false", "no", "disable", "disabled"].includes(normalized)) return false;
  return "unknown";
}

function normalizeUsage(stats: unknown) {
  if (!stats || typeof stats !== "object") return undefined;
  const record = stats as Record<string, unknown>;
  const tokens =
    record.tokens && typeof record.tokens === "object"
      ? (record.tokens as Record<string, unknown>)
      : {};
  const contextUsage =
    record.contextUsage && typeof record.contextUsage === "object"
      ? (record.contextUsage as Record<string, unknown>)
      : {};
  const numberValue = (value: unknown) =>
    typeof value === "number" && Number.isFinite(value) ? value : undefined;
  const inputTokens = numberValue(tokens.input) ?? 0;
  const outputTokens = numberValue(tokens.output) ?? 0;
  const cacheReadTokens = numberValue(tokens.cacheRead) ?? 0;
  const cacheWriteTokens = numberValue(tokens.cacheWrite) ?? 0;
  const totalTokens =
    numberValue(tokens.total) ?? inputTokens + outputTokens + cacheReadTokens + cacheWriteTokens;
  if (totalTokens <= 0) return undefined;
  return {
    usedTokens: totalTokens,
    totalProcessedTokens: totalTokens,
    inputTokens,
    outputTokens,
    cachedInputTokens: cacheReadTokens,
    maxTokens: numberValue(contextUsage.contextWindow),
    lastUsedTokens: totalTokens,
    lastInputTokens: inputTokens,
    lastCachedInputTokens: cacheReadTokens,
    lastOutputTokens: outputTokens,
    compactsAutomatically: true,
  };
}

async function readSessionFile(client: PiRpcClient): Promise<string | undefined> {
  try {
    return sessionFileFromState(await client.send({ type: "get_state" }));
  } catch {
    return undefined;
  }
}

function isPiMcpAdapterCommand(command: unknown): boolean {
  if (!command || typeof command !== "object") return false;
  const record = command as Record<string, unknown>;
  if (record.source !== "extension" || typeof record.name !== "string" || !/^mcp(?::\d+)?$/.test(record.name)) {
    return false;
  }
  return record.sourceInfo !== undefined && JSON.stringify(record.sourceInfo).includes("pi-mcp-adapter");
}

async function detectPiMcpAdapter(input: {
  readonly binaryPath: string;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv;
}): Promise<boolean> {
  let client: PiRpcClient | undefined;
  try {
    client = await startPiRpcProcess(input, () => undefined);
    const data = await client.send({ type: "get_commands" });
    const commands = data && typeof data === "object" ? (data as Record<string, unknown>).commands : undefined;
    return Array.isArray(commands) && commands.some(isPiMcpAdapterCommand);
  } catch {
    return false;
  } finally {
    await client?.stop();
  }
}

async function writeT3McpConfig(
  options: PiAdapterLiveOptions,
  threadId: ThreadId,
): Promise<string | undefined> {
  const mcpSession = McpProviderSession.readMcpProviderSession(threadId);
  if (!mcpSession) return undefined;
  const baseDir = options.stateDir
    ? NodePath.join(options.stateDir, "pi-mcp")
    : NodePath.join(NodeOS.tmpdir(), "t3-code-pi-mcp");
  await NodeFSP.mkdir(baseDir, { recursive: true });
  const configPath = NodePath.join(
    baseDir,
    `${String(threadId).replace(/[^a-z0-9_-]/gi, "-")}-${NodeCrypto.randomUUID()}.json`,
  );
  await NodeFSP.writeFile(
    configPath,
    JSON.stringify(
      {
        mcpServers: {
          "t3-code": {
            url: mcpSession.endpoint,
            headers: { Authorization: mcpSession.authorizationHeader },
            auth: false,
            oauth: false,
          },
        },
      },
      null,
      2,
    ),
  );
  return configPath;
}

async function createT3PiExtensionFile(
  stateDir: string | undefined,
): Promise<{ path: string; cleanupPath: string }> {
  const baseDir = stateDir
    ? NodePath.join(stateDir, "pi-extension")
    : await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-code-pi-extension-"));
  await NodeFSP.mkdir(baseDir, { recursive: true });
  const filePath = NodePath.join(baseDir, `t3-pi-${NodeCrypto.randomUUID()}.mjs`);
  await NodeFSP.writeFile(
    filePath,
    `
function decodePayload(encoded) {
  return JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
}
function readTextContent(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .filter((part) => part && part.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .join("\\n\\n");
}
function getCapturedUserEntries(ctx) {
  return ctx.sessionManager
    .getEntries()
    .filter((entry) => entry.type === "message" && entry.message?.role === "user")
    .map((entry) => ({
      id: entry.id,
      parentId: entry.parentId ?? null,
      text: readTextContent(entry.message.content),
    }));
}
function emitEntryCapture(ctx, reason, requestId) {
  ctx.ui.notify(
    "${T3_PI_ENTRY_CAPTURE_MARKER} " +
      JSON.stringify({ reason, requestId, entries: getCapturedUserEntries(ctx) }),
    "info",
  );
}
function emitCommandResult(ctx, requestId, result) {
  ctx.ui.notify(
    "${T3_PI_COMMAND_RESULT_MARKER} " + JSON.stringify({ requestId, ...result }),
    result.ok ? "info" : "error",
  );
}
export default function t3PiIntegration(pi) {
  pi.on("session_start", async (_event, ctx) => emitEntryCapture(ctx, "session_start"));
  pi.on("turn_end", async (_event, ctx) => emitEntryCapture(ctx, "turn_end"));
  pi.registerCommand("${T3_PI_CAPTURE_COMMAND}", {
    description: "Internal T3 Code entry capture bridge",
    handler: async (args, ctx) => {
      const payload = decodePayload(args.trim());
      emitEntryCapture(ctx, "command", payload.requestId);
    },
  });
  pi.registerCommand("${T3_PI_TREE_COMMAND}", {
    description: "Internal T3 Code tree navigation bridge",
    handler: async (args, ctx) => {
      const payload = decodePayload(args.trim());
      try {
        const result = await ctx.navigateTree(payload.targetId, { summarize: false });
        emitEntryCapture(ctx, "tree_navigation");
        emitCommandResult(ctx, payload.requestId, { ok: true, result });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        emitCommandResult(ctx, payload.requestId, { ok: false, error: message });
        throw error;
      }
    },
  });
}
`.trimStart(),
    "utf8",
  );
  return { path: filePath, cleanupPath: stateDir ? filePath : baseDir };
}

async function attachmentImages(input: {
  readonly attachmentsDir: string | undefined;
  readonly attachments: ReadonlyArray<ChatAttachment> | undefined;
}): Promise<PiImageContent[]> {
  if (!input.attachments?.length) return [];
  if (!input.attachmentsDir)
    throw new Error("Pi image attachments require a configured attachments directory.");
  const images: PiImageContent[] = [];
  for (const attachment of input.attachments) {
    const attachmentPath = resolveAttachmentPath({
      attachmentsDir: input.attachmentsDir,
      attachment,
    });
    if (!attachmentPath) throw new Error(`Invalid attachment id '${attachment.id}'.`);
    const bytes = await NodeFSP.readFile(attachmentPath);
    images.push({ type: "image", data: bytes.toString("base64"), mimeType: attachment.mimeType });
  }
  return images;
}

function parseMarkerPayload(message: string, marker: string): Record<string, unknown> | null {
  const prefix = `${marker} `;
  if (!message.startsWith(prefix)) return null;
  try {
    const parsed = JSON.parse(message.slice(prefix.length)) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

function parseCapturedEntries(value: unknown): PiCapturedEntry[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((entry): PiCapturedEntry[] => {
    if (!entry || typeof entry !== "object") return [];
    const record = entry as Record<string, unknown>;
    if (typeof record.id !== "string" || typeof record.text !== "string") return [];
    return [
      {
        id: record.id,
        text: record.text,
        parentId: typeof record.parentId === "string" ? record.parentId : null,
      },
    ];
  });
}

function userInputPayloadForExtensionRequest(record: Record<string, unknown>) {
  const title =
    typeof record.title === "string" && record.title.trim()
      ? record.title.trim()
      : `Pi ${String(record.method ?? "input")}`;
  const options = Array.isArray(record.options)
    ? record.options.filter(
        (item): item is string => typeof item === "string" && item.trim().length > 0,
      )
    : [];
  return {
    questions: [
      {
        id: "value",
        header: title,
        question:
          typeof record.message === "string" && record.message.trim()
            ? record.message.trim()
            : title,
        options: options.map((label) => ({ label, description: label })),
        multiSelect: false,
      },
    ],
  };
}

export const makePiAdapter = (
  settings: PiSettings,
  options: PiAdapterLiveOptions = {},
): Effect.Effect<ProviderAdapterShape<PiAdapterError>> =>
  Effect.gen(function* () {
    const events = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<ThreadId, PiSessionContext>();
    const instanceId = options.instanceId;

    const emit = (event: ProviderRuntimeEvent) => Effect.runFork(Queue.offer(events, event));
    const stamp = (threadId: ThreadId, turnId?: TurnId) => ({
      eventId: eventId(),
      provider: PROVIDER,
      ...(instanceId ? { providerInstanceId: instanceId } : {}),
      threadId,
      ...(turnId ? { turnId } : {}),
      createdAt: new Date().toISOString(),
    });

    const requireSession = (threadId: ThreadId) => {
      const ctx = sessions.get(threadId);
      if (!ctx) {
        throw new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId });
      }
      return ctx;
    };

    const emitUsageAfterTurn = async (
      threadId: ThreadId,
      ctx: PiSessionContext,
      turnId: TurnId,
    ) => {
      try {
        const usage = normalizeUsage(await ctx.client.send({ type: "get_session_stats" }));
        if (usage) {
          emit({
            type: "thread.token-usage.updated",
            ...stamp(threadId, turnId),
            payload: { usage },
          } as ProviderRuntimeEvent);
        }
      } catch {
        // Older Pi builds may not expose get_session_stats.
      }
      const sessionFile = await readSessionFile(ctx.client);
      if (sessionFile) {
        ctx.session = {
          ...ctx.session,
          resumeCursor: { sessionFile },
          updatedAt: new Date().toISOString(),
        };
      }
    };

    const appendMissingAssistantText = (streamed: string, finalText: string): string => {
      if (finalText.startsWith(streamed)) return finalText.slice(streamed.length);
      if (streamed.length === 0) return finalText;
      const maxOverlap = Math.min(streamed.length, finalText.length);
      for (let size = maxOverlap; size > 0; size -= 1) {
        if (streamed.slice(-size) === finalText.slice(0, size)) {
          return finalText.slice(size);
        }
      }
      // If Pi's final transcript is not an extension of the streamed text, prefer
      // surfacing the canonical final answer over silently dropping it. Prefix with
      // a separator so the mismatch is readable rather than welded to a partial word.
      return `\n\n${finalText}`;
    };

    const reconcileAssistantText = (
      threadId: ThreadId,
      ctx: PiSessionContext,
      turnId: TurnId,
      message: unknown,
      raw: unknown,
    ) => {
      const finalText = assistantTextFromPiMessage(message);
      if (!finalText) return;
      const streamed = ctx.assistantTextByTurn.get(turnId) ?? "";
      if (streamed === finalText) return;
      const missing = appendMissingAssistantText(streamed, finalText);
      if (missing.length === 0) return;
      ctx.assistantTextByTurn.set(turnId, `${streamed}${missing}`);
      emit({
        type: "content.delta",
        ...stamp(threadId, turnId),
        payload: { streamKind: "assistant_text", delta: missing },
        raw: { source: "pi.rpc", payload: raw },
      } as ProviderRuntimeEvent);
    };

    const completeReasoningTask = (
      threadId: ThreadId,
      ctx: PiSessionContext,
      turnId: TurnId,
      raw: unknown,
    ) => {
      const summary = ctx.reasoningSummaryByTurn.get(turnId);
      if (!summary) return;
      emit({
        type: "task.completed",
        ...stamp(threadId, turnId),
        payload: {
          taskId: piThinkingTaskId(turnId),
          status: "completed",
          summary,
        },
        raw: { source: "pi.rpc", payload: raw },
      } as ProviderRuntimeEvent);
      ctx.reasoningSummaryByTurn.delete(turnId);
    };

    const completeSuccessfulTurn = (
      threadId: ThreadId,
      ctx: PiSessionContext,
      turnId: TurnId,
      raw: unknown,
    ) => {
      completeReasoningTask(threadId, ctx, turnId, raw);
      emit({
        type: "turn.completed",
        ...stamp(threadId, turnId),
        payload: { state: "completed", stopReason: "stop" },
        raw: { source: "pi.rpc", payload: raw },
      } as ProviderRuntimeEvent);
      ctx.activeTurnId = undefined;
      const { activeTurnId, ...sessionWithoutActiveTurn } = ctx.session;
      void activeTurnId;
      ctx.session = {
        ...sessionWithoutActiveTurn,
        status: "ready",
        updatedAt: new Date().toISOString(),
      };
      void emitUsageAfterTurn(threadId, ctx, turnId);
    };

    const handlePiEvent = (threadId: ThreadId, raw: unknown) => {
      const ctx = sessions.get(threadId);
      const turnId = ctx?.activeTurnId;
      if (!ctx) return;
      const text = textFromEvent(raw);
      if (text && turnId) {
        if (text.kind === "assistant_text") {
          ctx.assistantTextByTurn.set(
            turnId,
            `${ctx.assistantTextByTurn.get(turnId) ?? ""}${text.delta}`,
          );
        }
        emit({
          type: "content.delta",
          ...stamp(threadId, turnId),
          payload: {
            streamKind: text.kind,
            delta: text.delta,
            ...(typeof text.index === "number" ? { contentIndex: text.index } : {}),
          },
          raw: { source: "pi.rpc", payload: raw },
        } as ProviderRuntimeEvent);
        if (text.kind === "reasoning_text" && text.delta.trim().length > 0) {
          const previous = ctx.reasoningSummaryByTurn.get(turnId) ?? "";
          const nextSummary =
            previous === "Pi started working" ? text.delta.trim() : `${previous}${text.delta}`.trim();
          ctx.reasoningSummaryByTurn.set(turnId, nextSummary);
          emit({
            type: "task.progress",
            ...stamp(threadId, turnId),
            payload: {
              taskId: piThinkingTaskId(turnId),
              taskType: "reasoning",
              description: text.delta,
              summary: text.delta,
            },
            raw: { source: "pi.rpc", payload: raw },
          } as ProviderRuntimeEvent);
        }
        return;
      }
      if (!raw || typeof raw !== "object") return;
      const record = raw as Record<string, unknown>;
      if (record.type === "process_exit") {
        const error = typeof record.error === "string" ? record.error : "Pi RPC process exited.";
        if (turnId) {
          completeReasoningTask(threadId, ctx, turnId, raw);
          emit({
            type: "turn.completed",
            ...stamp(threadId, turnId),
            payload: { state: "failed", stopReason: "process_exit", errorMessage: error },
            raw: { source: "pi.rpc", payload: raw },
          } as ProviderRuntimeEvent);
          ctx.activeTurnId = undefined;
        }
        const { activeTurnId: _activeTurnId, ...sessionWithoutActiveTurn } = ctx.session;
        void _activeTurnId;
        ctx.session = {
          ...sessionWithoutActiveTurn,
          status: "error",
          lastError: error,
          updatedAt: new Date().toISOString(),
        };
        emit({
          type: "session.exited",
          ...stamp(threadId, turnId),
          payload: { exitKind: "error", reason: error, recoverable: true },
          raw: { source: "pi.rpc", payload: raw },
        } as ProviderRuntimeEvent);
        return;
      }
      if (record.type === "message_end" && turnId) {
        reconcileAssistantText(threadId, ctx, turnId, record.message, raw);
        const message =
          record.message && typeof record.message === "object"
            ? (record.message as Record<string, unknown>)
            : null;
        if (message?.role === "custom") {
          completeSuccessfulTurn(threadId, ctx, turnId, raw);
          return;
        }
      }
      if (record.type === "extension_ui_request" && record.method === "notify") {
        const message = typeof record.message === "string" ? record.message : "";
        const capture = parseMarkerPayload(message, T3_PI_ENTRY_CAPTURE_MARKER);
        if (capture) {
          ctx.capturedUserEntries = parseCapturedEntries(capture.entries);
          const requestId = typeof capture.requestId === "string" ? capture.requestId : undefined;
          if (requestId) {
            ctx.pendingExtensionResults.get(requestId)?.resolve(ctx.capturedUserEntries);
            ctx.pendingExtensionResults.delete(requestId);
          }
          return;
        }
        const result = parseMarkerPayload(message, T3_PI_COMMAND_RESULT_MARKER);
        if (result && typeof result.requestId === "string") {
          const pending = ctx.pendingExtensionResults.get(result.requestId);
          if (pending) {
            ctx.pendingExtensionResults.delete(result.requestId);
            if (result.ok === true) pending.resolve(result.result);
            else pending.reject(new Error(typeof result.error === "string" ? result.error : "Pi extension command failed"));
          }
          return;
        }
        // Pi notifications are fire-and-forget UI messages, not questions for the user.
        // Surface non-internal notifications as Work Log trace entries so the run
        // still explains what Pi is doing without showing a phantom input prompt.
        if (message.trim().length > 0) {
          emit({
            type: "task.progress",
            ...stamp(threadId, turnId),
            payload: {
              taskId: piNotificationTaskId(turnId, ++ctx.notificationSequence),
              taskType: "notification",
              description: message,
              summary: message,
            },
            raw: { source: "pi.rpc", payload: raw },
          } as ProviderRuntimeEvent);
        }
        return;
      }
      if (record.type === "extension_ui_request" && typeof record.id === "string") {
        const method = typeof record.method === "string" ? record.method : "unknown";
        const requestId = runtimeRequestId(record.id);
        if (method === "confirm") {
          ctx.pendingExtensionRequests.set(record.id, { method, kind: "request" });
          emit({
            type: "request.opened",
            ...stamp(threadId, turnId),
            requestId,
            payload: {
              requestType: "unknown",
              detail: typeof record.message === "string" ? record.message : `Pi ${method} request`,
              args: record,
            },
            raw: { source: "pi.rpc", payload: raw },
          } as ProviderRuntimeEvent);
          return;
        }
        if (method === "select" || method === "input" || method === "editor") {
          ctx.pendingExtensionRequests.set(record.id, { method, kind: "user-input" });
          emit({
            type: "user-input.requested",
            ...stamp(threadId, turnId),
            requestId,
            payload: userInputPayloadForExtensionRequest(record),
            raw: { source: "pi.rpc", payload: raw },
          } as ProviderRuntimeEvent);
          return;
        }
        // Unsupported/fire-and-forget Pi UI methods are trace entries, not questions.
        emit({
          type: "task.progress",
          ...stamp(threadId, turnId),
          payload: {
            taskId: piNotificationTaskId(turnId, ++ctx.notificationSequence),
            taskType: "notification",
            description:
              typeof record.message === "string" && record.message.trim().length > 0
                ? record.message
                : `Pi UI event: ${method}`,
            summary:
              typeof record.message === "string" && record.message.trim().length > 0
                ? record.message
                : `Pi UI event: ${method}`,
          },
          raw: { source: "pi.rpc", payload: raw },
        } as ProviderRuntimeEvent);
        return;
      }
      if (!turnId) return;
      if (record.type === "agent_end" || record.type === "turn_end") {
        if (Array.isArray(record.messages)) {
          const finalAssistant = [...record.messages]
            .reverse()
            .find((message) => assistantTextFromPiMessage(message) !== undefined);
          reconcileAssistantText(threadId, ctx, turnId, finalAssistant, raw);
        }
        completeSuccessfulTurn(threadId, ctx, turnId, raw);
        return;
      }
      if (record.type === "agent_start" || record.type === "turn_start") {
        emit({
          type: "session.state.changed",
          ...stamp(threadId, turnId),
          payload: { state: "running" },
          raw: { source: "pi.rpc", payload: raw },
        } as ProviderRuntimeEvent);
        const startSummary = "Pi started working";
        if (!ctx.reasoningSummaryByTurn.has(turnId)) {
          ctx.reasoningSummaryByTurn.set(turnId, startSummary);
          emit({
            type: "task.progress",
            ...stamp(threadId, turnId),
            payload: {
              taskId: piThinkingTaskId(turnId),
              taskType: "reasoning",
              description: startSummary,
              summary: startSummary,
            },
            raw: { source: "pi.rpc", payload: raw },
          } as ProviderRuntimeEvent);
        }
        return;
      }
      if (record.type === "compaction_start") {
        emit({
          type: "item.started",
          ...stamp(threadId, turnId),
          itemId: runtimeItemId(`compaction-${turnId}`),
          payload: {
            itemType: "dynamic_tool_call",
            status: "inProgress",
            title: "Compacting context",
            data: record,
          },
          raw: { source: "pi.rpc", payload: raw },
        } as ProviderRuntimeEvent);
        return;
      }
      if (record.type === "compaction_end") {
        emit({
          type: "item.completed",
          ...stamp(threadId, turnId),
          itemId: runtimeItemId(`compaction-${turnId}`),
          payload: {
            itemType: "dynamic_tool_call",
            status: record.errorMessage ? "failed" : "completed",
            title: "Compacted context",
            data: record,
          },
          raw: { source: "pi.rpc", payload: raw },
        } as ProviderRuntimeEvent);
        return;
      }
      if (
        record.type === "tool_execution_start" ||
        record.type === "tool_execution_update" ||
        record.type === "tool_execution_end"
      ) {
        const toolCallId =
          typeof record.toolCallId === "string" ? record.toolCallId : NodeCrypto.randomUUID();
        const rawToolName = typeof record.toolName === "string" ? record.toolName : "Tool";
        const toolName = toolTitle(record);
        const toolDetail = toolDetailFromArgs(rawToolName, record.args);
        const resultDetail = toolDetailFromResult(record.result);
        const base = {
          ...stamp(threadId, turnId),
          itemId: runtimeItemId(toolCallId),
          providerRefs: { providerItemId: toolCallId },
          raw: { source: "pi.rpc", payload: raw },
        };
        if (record.type === "tool_execution_start") {
          const toolSummary = toolDetail ? `Using ${toolName}: ${toolDetail}` : `Using ${toolName}`;
          ctx.reasoningSummaryByTurn.set(turnId, toolSummary);
          emit({
            type: "task.progress",
            ...stamp(threadId, turnId),
            payload: {
              taskId: piThinkingTaskId(turnId),
              taskType: "reasoning",
              description: toolSummary,
              summary: toolSummary,
              lastToolName: toolName,
            },
            raw: { source: "pi.rpc", payload: raw },
          } as ProviderRuntimeEvent);
          emit({
            type: "item.started",
            ...base,
            payload: {
              itemType: toToolItemType(toolName),
              status: "inProgress",
              title: toolName,
              ...(toolDetail ? { detail: toolDetail } : {}),
              data: { toolCallId, args: record.args },
            },
          } as ProviderRuntimeEvent);
        } else if (record.type === "tool_execution_update") {
          emit({
            type: "tool.progress",
            ...base,
            payload: { toolUseId: toolCallId, toolName, summary: "Running" },
          } as ProviderRuntimeEvent);
        } else {
          emit({
            type: "item.completed",
            ...base,
            payload: {
              itemType: toToolItemType(toolName),
              status: record.isError === true ? "failed" : "completed",
              title: toolName,
              ...(resultDetail ? { detail: resultDetail } : {}),
              data: { toolCallId, result: record.result },
            },
          } as ProviderRuntimeEvent);
        }
      }
    };

    const startSession: ProviderAdapterShape<PiAdapterError>["startSession"] = (input) =>
      Effect.tryPromise({
        try: async () => {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            throw new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          const cwd = input.cwd?.trim();
          if (!cwd) {
            throw new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }
          const existing = sessions.get(input.threadId);
          if (existing) {
            await existing.client.stop();
            await Promise.all([...existing.tempFiles].map((file) => NodeFSP.rm(file, { force: true, recursive: true })));
          }
          const tempFiles = new Set<string>();
          const args: string[] = ["--append-system-prompt", T3_PI_SYSTEM_PROMPT];
          const sessionFile = resumeSessionFile(input.resumeCursor);
          if (sessionFile) args.push("--session", sessionFile);
          if (input.modelSelection?.model) args.push("--model", input.modelSelection.model);
          const thinkingLevel = selectedThinkingLevel(input.modelSelection?.options);
          if (thinkingLevel) args.push("--thinking", thinkingLevel);
          const hasT3McpSession = McpProviderSession.readMcpProviderSession(input.threadId) !== undefined;
          const mcpConfigPath =
            hasT3McpSession &&
            (await detectPiMcpAdapter({
              binaryPath: settings.binaryPath,
              cwd,
              ...(options.environment ? { environment: options.environment } : {}),
            }))
              ? await writeT3McpConfig(options, input.threadId)
              : undefined;
          if (mcpConfigPath) {
            args.push("--mcp-config", mcpConfigPath);
            tempFiles.add(mcpConfigPath);
          }
          const configuredExtensionPath = options.environment?.T3_PI_EXTENSION_PATH;
          const generatedExtension = configuredExtensionPath
            ? undefined
            : await createT3PiExtensionFile(options.stateDir);
          const extensionPath = configuredExtensionPath ?? generatedExtension?.path;
          if (!extensionPath) {
            throw new Error("Pi extension path was not generated.");
          }
          args.push("--extension", extensionPath);
          if (generatedExtension) tempFiles.add(generatedExtension.cleanupPath);
          let client: PiRpcClient | undefined;
          try {
            client = await startPiRpcProcess(
              {
                binaryPath: settings.binaryPath,
                cwd,
                environment: options.environment,
                args,
              },
              (event) => handlePiEvent(input.threadId, event),
            );
          } catch (cause) {
            await Promise.all([...tempFiles].map((file) => NodeFSP.rm(file, { force: true, recursive: true })));
            throw cause;
          }
          const createdAt = new Date().toISOString();
          const currentSessionFile = (await readSessionFile(client)) ?? sessionFile;
          const session: ProviderSession = {
            provider: PROVIDER,
            ...(instanceId ? { providerInstanceId: instanceId } : {}),
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd,
            ...(input.modelSelection?.model ? { model: input.modelSelection.model } : {}),
            threadId: input.threadId,
            ...(currentSessionFile ? { resumeCursor: { sessionFile: currentSessionFile } } : {}),
            createdAt,
            updatedAt: createdAt,
          };
          sessions.set(input.threadId, {
            session,
            client,
            turns: [],
            pendingExtensionRequests: new Map(),
            tempFiles,
            capturedUserEntries: [],
            pendingExtensionResults: new Map(),
            assistantTextByTurn: new Map(),
            reasoningSummaryByTurn: new Map(),
            notificationSequence: 0,
          });
          emit({
            type: "session.started",
            ...stamp(input.threadId),
            payload: currentSessionFile ? { resume: { sessionFile: currentSessionFile } } : {},
          } as ProviderRuntimeEvent);
          emit({
            type: "session.state.changed",
            ...stamp(input.threadId),
            payload: { state: "ready", reason: "Pi RPC session ready" },
          } as ProviderRuntimeEvent);
          emit({
            type: "thread.started",
            ...stamp(input.threadId),
            payload: {},
          } as ProviderRuntimeEvent);
          return session;
        },
        catch: (cause) =>
          cause instanceof ProviderAdapterValidationError
            ? cause
            : new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "startSession",
                detail: cause instanceof Error ? cause.message : String(cause),
                cause,
              }),
      });

    const sendTurn: ProviderAdapterShape<PiAdapterError>["sendTurn"] = (input) => {
      let startedCtx: PiSessionContext | undefined;
      let startedTurnId: TurnId | undefined;
      return Effect.tryPromise({
        try: async () => {
          const ctx = requireSession(input.threadId);
          const message = input.input?.trim();
          const images = await attachmentImages({
            attachmentsDir: options.attachmentsDir,
            attachments: input.attachments,
          });
          if (!message && images.length === 0) {
            throw new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Turn requires non-empty text or attachments.",
            });
          }
          const command = parseSlashCommand(message);
          if (command?.name === "compact") {
            await ctx.client.send({
              type: "compact",
              ...(command.args ? { customInstructions: command.args } : {}),
            });
            return {
              threadId: input.threadId,
              turnId: ctx.activeTurnId ?? TurnId.make(NodeCrypto.randomUUID()),
              resumeCursor: ctx.session.resumeCursor,
            };
          }
          if (command?.name === "autocompact") {
            let enabled = parseAutoCompactMode(command.args);
            if (enabled === "unknown") {
              throw new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "sendTurn",
                issue: "Usage: /autocompact [on|off|toggle].",
              });
            }
            if (enabled === "toggle") {
              const state = await ctx.client.send({ type: "get_state" });
              const current =
                state && typeof state === "object"
                  ? (state as Record<string, unknown>).autoCompactionEnabled
                  : undefined;
              enabled = typeof current === "boolean" ? !current : true;
            }
            await ctx.client.send({ type: "set_auto_compaction", enabled });
            const turnId = TurnId.make(NodeCrypto.randomUUID());
            emit({
              type: "turn.started",
              ...stamp(input.threadId, turnId),
              payload: {},
            } as ProviderRuntimeEvent);
            emit({
              type: "content.delta",
              ...stamp(input.threadId, turnId),
              payload: {
                streamKind: "assistant_text",
                delta: `Auto-compaction ${enabled ? "enabled" : "disabled"}.`,
              },
            } as ProviderRuntimeEvent);
            emit({
              type: "turn.completed",
              ...stamp(input.threadId, turnId),
              payload: { state: "completed", stopReason: "stop" },
            } as ProviderRuntimeEvent);
            return { threadId: input.threadId, turnId, resumeCursor: ctx.session.resumeCursor };
          }
          const turnId = TurnId.make(NodeCrypto.randomUUID());
          startedCtx = ctx;
          startedTurnId = turnId;
          ctx.activeTurnId = turnId;
          ctx.turns.push({ id: turnId, items: [] });
          ctx.session = {
            ...ctx.session,
            status: "running",
            activeTurnId: turnId,
            updatedAt: new Date().toISOString(),
          };
          emit({
            type: "turn.started",
            ...stamp(input.threadId, turnId),
            payload: input.modelSelection?.model ? { model: input.modelSelection.model } : {},
          } as ProviderRuntimeEvent);
          const startSummary = "Pi started working";
          ctx.reasoningSummaryByTurn.set(turnId, startSummary);
          emit({
            type: "task.progress",
            ...stamp(input.threadId, turnId),
            payload: {
              taskId: piThinkingTaskId(turnId),
              taskType: "reasoning",
              description: startSummary,
              summary: startSummary,
            },
          } as ProviderRuntimeEvent);
          await ctx.client.send({
            type: "prompt",
            message: message ?? "",
            ...(images.length > 0 ? { images } : {}),
          });
          return { threadId: input.threadId, turnId, resumeCursor: ctx.session.resumeCursor };
        },
        catch: (cause) => {
          if (
            cause instanceof ProviderAdapterSessionNotFoundError ||
            cause instanceof ProviderAdapterValidationError
          ) {
            return cause;
          }
          const detail = cause instanceof Error ? cause.message : String(cause);
          if (startedCtx && startedTurnId && startedCtx.activeTurnId === startedTurnId) {
            completeReasoningTask(input.threadId, startedCtx, startedTurnId, { type: "prompt_error", detail });
            emit({
              type: "turn.completed",
              ...stamp(input.threadId, startedTurnId),
              payload: { state: "failed", stopReason: "prompt_error", errorMessage: detail },
            } as ProviderRuntimeEvent);
            startedCtx.activeTurnId = undefined;
            const { activeTurnId: _activeTurnId, ...sessionWithoutActiveTurn } = startedCtx.session;
            void _activeTurnId;
            startedCtx.session = {
              ...sessionWithoutActiveTurn,
              status: "error",
              lastError: detail,
              updatedAt: new Date().toISOString(),
            };
          }
          return new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "prompt",
            detail,
            cause,
          });
        },
      });
    };

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "unsupported" },
      startSession,
      sendTurn,
      interruptTurn: (threadId, turnId) =>
        Effect.tryPromise({
          try: async () => {
            const ctx = requireSession(threadId);
            await ctx.client.send({ type: "abort" });
            const activeTurnId = turnId ?? ctx.activeTurnId;
            if (activeTurnId) {
              completeReasoningTask(threadId, ctx, activeTurnId, { type: "abort" });
              emit({
                type: "turn.aborted",
                ...stamp(threadId, activeTurnId),
                payload: { reason: "Interrupted by user" },
              } as ProviderRuntimeEvent);
              emit({
                type: "turn.completed",
                ...stamp(threadId, activeTurnId),
                payload: { state: "cancelled", stopReason: "abort" },
              } as ProviderRuntimeEvent);
              ctx.activeTurnId = undefined;
              const { activeTurnId: _activeTurnId, ...sessionWithoutActiveTurn } = ctx.session;
              void _activeTurnId;
              ctx.session = {
                ...sessionWithoutActiveTurn,
                status: "ready",
                updatedAt: new Date().toISOString(),
              };
            }
          },
          catch: (cause) =>
            cause instanceof ProviderAdapterSessionNotFoundError
              ? cause
              : new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "abort",
                  detail: cause instanceof Error ? cause.message : String(cause),
                  cause,
                }),
        }),
      respondToRequest: (threadId, requestId, decision) =>
        Effect.tryPromise({
          try: async () => {
            const ctx = requireSession(threadId);
            const id = String(requestId);
            ctx.client.notify({
              type: "extension_ui_response",
              id,
              confirmed: decision === "accept" || decision === "acceptForSession",
              cancelled: decision === "cancel" || decision === "decline",
            });
            ctx.pendingExtensionRequests.delete(id);
            emit({
              type: "request.resolved",
              ...stamp(threadId, ctx.activeTurnId),
              requestId: runtimeRequestId(id),
              payload: { requestType: "unknown", decision, resolution: { decision } },
            } as ProviderRuntimeEvent);
          },
          catch: (cause) =>
            cause instanceof ProviderAdapterSessionNotFoundError
              ? cause
              : new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "respondToRequest",
                  detail: cause instanceof Error ? cause.message : String(cause),
                  cause,
                }),
        }),
      respondToUserInput: (threadId, requestId, answers) =>
        Effect.tryPromise({
          try: async () => {
            const ctx = requireSession(threadId);
            const id = String(requestId);
            const value = answers.value;
            const responseValue = Array.isArray(value)
              ? value.join(", ")
              : typeof value === "string"
                ? value
                : JSON.stringify(answers);
            ctx.client.notify({ type: "extension_ui_response", id, value: responseValue });
            ctx.pendingExtensionRequests.delete(id);
            emit({
              type: "user-input.resolved",
              ...stamp(threadId, ctx.activeTurnId),
              requestId: runtimeRequestId(id),
              payload: { answers },
            } as ProviderRuntimeEvent);
          },
          catch: (cause) =>
            cause instanceof ProviderAdapterSessionNotFoundError
              ? cause
              : new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "respondToUserInput",
                  detail: cause instanceof Error ? cause.message : String(cause),
                  cause,
                }),
        }),
      stopSession: (threadId) =>
        Effect.tryPromise({
          try: async () => {
            const ctx = sessions.get(threadId);
            if (!ctx) return;
            sessions.delete(threadId);
            await ctx.client.stop();
            await Promise.allSettled(
              [...ctx.tempFiles].map((file) => NodeFSP.rm(file, { force: true, recursive: true })),
            );
            emit({
              type: "session.exited",
              ...stamp(threadId),
              payload: { exitKind: "graceful" },
            } as ProviderRuntimeEvent);
          },
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "stopSession",
              detail: cause instanceof Error ? cause.message : String(cause),
              cause,
            }),
        }),
      listSessions: () => Effect.succeed([...sessions.values()].map((ctx) => ctx.session)),
      hasSession: (threadId) => Effect.succeed(sessions.has(threadId)),
      readThread: (threadId) =>
        Effect.sync(() => {
          const ctx = requireSession(threadId);
          return { threadId, turns: ctx.turns };
        }),
      rollbackThread: (threadId, numTurns) =>
        Effect.tryPromise({
          try: async () => {
            const ctx = requireSession(threadId);
            if (ctx.activeTurnId) {
              throw new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "rollbackThread",
                issue: "Cannot roll back a Pi session while a turn is active.",
              });
            }
            if (numTurns <= 0 || ctx.capturedUserEntries.length === 0) {
              return { threadId, turns: ctx.turns };
            }
            const targetIndex = Math.max(0, ctx.capturedUserEntries.length - numTurns);
            const targetEntry = ctx.capturedUserEntries[targetIndex];
            const targetId = targetEntry?.parentId;
            if (!targetId) {
              ctx.turns = [];
              return { threadId, turns: ctx.turns };
            }
            const requestId = NodeCrypto.randomUUID();
            const resultPromise = new Promise<unknown>((resolve, reject) => {
              ctx.pendingExtensionResults.set(requestId, { resolve, reject });
              setTimeout(() => {
                if (ctx.pendingExtensionResults.delete(requestId)) {
                  reject(new Error("Timed out waiting for Pi tree navigation."));
                }
              }, 30_000).unref();
            });
            const payload = Buffer.from(JSON.stringify({ targetId, requestId })).toString("base64url");
            await ctx.client.send({ type: "prompt", message: `/${T3_PI_TREE_COMMAND} ${payload}` });
            await resultPromise;
            ctx.turns = ctx.turns.slice(0, Math.max(0, ctx.turns.length - numTurns));
            return { threadId, turns: ctx.turns };
          },
          catch: (cause) => {
            if (
              cause instanceof ProviderAdapterSessionNotFoundError ||
              cause instanceof ProviderAdapterValidationError
            ) {
              return cause;
            }
            return new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "rollbackThread",
              detail: cause instanceof Error ? cause.message : String(cause),
              cause,
            });
          },
        }),
      stopAll: () =>
        Effect.promise(async () => {
          const all = [...sessions.values()];
          sessions.clear();
          await Promise.allSettled(
            all.flatMap((ctx) => [
              ctx.client.stop(),
              ...[...ctx.tempFiles].map((file) => NodeFSP.rm(file, { force: true, recursive: true })),
            ]),
          );
        }),
      streamEvents: Stream.fromQueue(events),
    } satisfies ProviderAdapterShape<PiAdapterError>;
  });
