import {
  ProviderDriverKind,
  type PiSettings,
  type ServerProviderModel,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  buildServerProvider,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import { probePiAvailableModels, probePiSlashCommands } from "./PiRpcProcess.ts";

const PROVIDER = ProviderDriverKind.make("pi");
const PI_PRESENTATION = {
  displayName: "Pi",
  badgeLabel: "Experimental",
  showInteractionModeToggle: false,
} as const;

const DEFAULT_PI_MODEL_CAPABILITIES = createModelCapabilities({ optionDescriptors: [] });
const BUILTIN_PI_SLASH_COMMANDS: ReadonlyArray<ServerProviderSlashCommand> = [
  {
    name: "compact",
    description: "Manually compact the Pi session context.",
    input: { hint: "[instructions]" },
  },
  {
    name: "autocompact",
    description: "Toggle automatic Pi context compaction.",
    input: { hint: "[on|off|toggle]" },
  },
];

function flattenPiSlashCommands(
  commands: ReadonlyArray<unknown>,
): ReadonlyArray<ServerProviderSlashCommand> {
  const byName = new Map<string, ServerProviderSlashCommand>();
  for (const command of BUILTIN_PI_SLASH_COMMANDS) byName.set(command.name, command);
  for (const value of commands) {
    if (!value || typeof value !== "object") continue;
    const record = value as Record<string, unknown>;
    const name = typeof record.name === "string" ? record.name.trim().replace(/^\//, "") : "";
    if (!name) continue;
    byName.set(name, {
      name,
      ...(typeof record.description === "string" && record.description.trim().length > 0
        ? { description: record.description.trim() }
        : {}),
    });
  }
  return [...byName.values()].toSorted((left, right) => left.name.localeCompare(right.name));
}

function flattenPiModels(models: ReadonlyArray<unknown>): ReadonlyArray<ServerProviderModel> {
  const flattened: ServerProviderModel[] = [];
  for (const value of models) {
    if (!value || typeof value !== "object") continue;
    const record = value as Record<string, unknown>;
    const provider = typeof record.provider === "string" ? record.provider : undefined;
    const id = typeof record.id === "string" ? record.id : undefined;
    if (!provider || !id) continue;
    const reasoning = typeof record.reasoning === "boolean" ? record.reasoning : false;
    flattened.push({
      slug: `${provider}/${id}`,
      name: id,
      subProvider: provider,
      isCustom: false,
      capabilities: createModelCapabilities({
        optionDescriptors: reasoning
          ? [
              {
                id: "thinkingLevel",
                label: "Thinking",
                type: "select" as const,
                options: [
                  { id: "low", label: "Low" },
                  { id: "medium", label: "Medium", isDefault: true as const },
                  { id: "high", label: "High" },
                ],
                currentValue: "medium",
              },
            ]
          : [],
      }),
    });
  }
  return flattened.toSorted((left, right) => {
    const providerCompare = (left.subProvider ?? "").localeCompare(right.subProvider ?? "");
    return providerCompare === 0 ? left.name.localeCompare(right.name) : providerCompare;
  });
}

export const makePendingPiProvider = (settings: PiSettings): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: providerModelsFromSettings(
        [],
        PROVIDER,
        settings.customModels,
        DEFAULT_PI_MODEL_CAPABILITIES,
      ),
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: settings.enabled
          ? "Pi provider status has not been checked in this session yet."
          : "Pi is disabled in T3 Code settings.",
      },
    });
  });

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  settings: PiSettings,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = providerModelsFromSettings(
    [],
    PROVIDER,
    settings.customModels,
    DEFAULT_PI_MODEL_CAPABILITIES,
  );

  if (!settings.enabled) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi is disabled in T3 Code settings.",
      },
    });
  }

  const versionExit = yield* Effect.exit(
    spawnAndCollect(
      settings.binaryPath,
      ChildProcess.make(
        settings.binaryPath,
        ["--version"],
        environment ? { env: environment, extendEnv: true } : {},
      ),
    ),
  );

  if (versionExit._tag === "Failure") {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Pi CLI is not installed or failed to run `pi --version`.",
      },
    });
  }

  const version = parseGenericCliVersion(versionExit.value.stdout || versionExit.value.stderr);
  const probeOptions = {
    binaryPath: settings.binaryPath,
    cwd,
    environment,
  };
  const modelsExit = yield* Effect.exit(probePiAvailableModels(probeOptions));
  const commandsExit = yield* Effect.exit(probePiSlashCommands(probeOptions));
  const slashCommands =
    commandsExit._tag === "Success"
      ? flattenPiSlashCommands(commandsExit.value)
      : BUILTIN_PI_SLASH_COMMANDS;

  if (modelsExit._tag === "Failure") {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      slashCommands,
      probe: {
        installed: true,
        version,
        status: "warning",
        auth: { status: "unknown" },
        message:
          "Pi is installed, but T3 Code could not probe available models via `get_available_models`.",
      },
    });
  }

  const models = providerModelsFromSettings(
    flattenPiModels(modelsExit.value),
    PROVIDER,
    settings.customModels,
    DEFAULT_PI_MODEL_CAPABILITIES,
  );
  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    slashCommands,
    probe: {
      installed: true,
      version,
      status: models.length > 0 ? "ready" : "warning",
      auth: { status: models.length > 0 ? "authenticated" : "unknown", type: "pi" },
      message:
        models.length > 0
          ? `Pi reported ${models.length} available model${models.length === 1 ? "" : "s"}.`
          : "Pi is installed, but did not report any available models.",
    },
  });
});
