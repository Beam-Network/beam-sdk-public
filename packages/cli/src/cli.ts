#!/usr/bin/env node
import {
  BEAM_DEFAULT_NATS_URL,
  BeamApiError,
  BeamClient,
  type DestConfig,
  type SourceConfig,
  type TransferStatusInfo
} from "@beam-network/sdk";

type ParsedOptions = Record<string, string | boolean | string[]>;

const terminalStates = new Set(["completed", "failed", "cancelled", "canceled"]);

async function main(argv = process.argv.slice(2)): Promise<void> {
  const command = argv[0];

  if (!command || command === "--help" || command === "-h" || command === "help") {
    printHelp();
    return;
  }

  try {
    if (command === "create") {
      await createCommand(argv.slice(1));
      return;
    }
    if (command === "status") {
      await statusCommand(argv.slice(1));
      return;
    }
    if (command === "distribute") {
      await distributeCommand(argv.slice(1));
      return;
    }
    if (command === "cancel") {
      await cancelCommand(argv.slice(1));
      return;
    }

    throw new CliError(`Unknown command: ${command}`);
  } catch (error) {
    if (error instanceof CliError) {
      console.error(`Error: ${error.message}`);
      process.exitCode = 1;
      return;
    }
    if (error instanceof BeamApiError) {
      console.error(`BEAM lifecycle error status=${error.status}`);
      process.exitCode = 1;
      return;
    }
    console.error(`Error: ${safeCliErrorText(error)}`);
    process.exitCode = 1;
  }
}

function safeErrorCode(error: unknown): string {
  if (!(error instanceof Error)) return "UnknownError";
  const record = error as Error & { code?: unknown; status?: unknown; statusCode?: unknown };
  const explicitCode = typeof record.code === "string" && /^[A-Za-z0-9_.-]{1,64}$/.test(record.code)
    ? record.code
    : undefined;
  const status = [record.status, record.statusCode]
    .find((value): value is number => Number.isInteger(value) && Number(value) >= 100 && Number(value) <= 599);
  return [explicitCode ?? error.name, status === undefined ? undefined : `status=${status}`]
    .filter(Boolean)
    .join(" ");
}

function safeCliErrorText(error: unknown): string {
  if (error instanceof Error && [
    "Beam SDK lifecycle endpoint must be nats://, tls://, or configured as natsWsUrl.",
    "transferRuntimeShardCount must be a positive integer.",
  ].includes(error.message)) {
    return error.message;
  }
  return safeErrorCode(error);
}

async function createCommand(args: string[]): Promise<void> {
  const options = parseOptions(args);
  const client = createClient(options);
  const sources = parseJsonList<SourceConfig>(options.source, "--source");
  const destinations = parseJsonList<DestConfig>(options.destination, "--destination");
  const totalSize = parseRequiredNumber(options["total-size"], "--total-size");

  const transfer = await client.createRawTransfer({
    sources,
    destinations,
    totalSize,
    chunkSize: parseOptionalNumber(options["chunk-size"], "--chunk-size"),
    name: parseOptionalString(options.name),
    testMode: Boolean(options["test-mode"]),
    progressiveMode: Boolean(options["progressive-mode"])
  });

  if (options.json) {
    printJson(transfer);
  } else {
    console.log(`Created transfer: ${transfer.transfer_id}`);
    console.log(`Chunks: ${transfer.total_chunks}`);
  }

  if (options.distribute) {
    const distributed = await client.distributeTransfer(transfer.transfer_id);
    if (options.json) {
      printJson({ distribute: distributed });
    } else {
      console.log(`Distributed transfer: ${distributed.transfer_id}`);
    }
  }

  if (options.wait) {
    const status = await waitForTransfer(
      client,
      transfer.transfer_id,
      parseOptionalNumber(options["poll-interval"], "--poll-interval") ?? 2
    );
    if (options.json) {
      printJson({ final_status: status });
    } else {
      printStatus(status);
    }
  }
}

async function statusCommand(args: string[]): Promise<void> {
  const { positional, options } = parseCommandArgs(args);
  const transferId = positional[0] ?? parseOptionalString(options["transfer-id"]);
  if (!transferId) {
    throw new CliError("Missing transfer id.");
  }

  const status = await createClient(options).transferStatus(transferId);
  if (options.json) {
    printJson(status);
  } else {
    printStatus(status);
  }
}

async function distributeCommand(args: string[]): Promise<void> {
  const { positional, options } = parseCommandArgs(args);
  const transferId = positional[0] ?? parseOptionalString(options["transfer-id"]);
  if (!transferId) {
    throw new CliError("Missing transfer id.");
  }

  const response = await createClient(options).distributeTransfer(transferId);
  if (options.json) {
    printJson(response);
  } else {
    console.log(`Distributed transfer: ${response.transfer_id}`);
  }
}

async function cancelCommand(args: string[]): Promise<void> {
  const { positional, options } = parseCommandArgs(args);
  const transferId = positional[0] ?? parseOptionalString(options["transfer-id"]);
  if (!transferId) {
    throw new CliError("Missing transfer id.");
  }

  const response = await createClient(options).cancelTransfer(transferId);
  if (options.json) {
    printJson(response);
  } else {
    console.log(response.message ?? "Transfer cancelled.");
  }
}

function createClient(options: ParsedOptions): BeamClient {
  const apiKey = parseOptionalString(options["api-key"]) ?? process.env.BEAM_API_KEY;
  const natsUrl = parseOptionalString(options.server) ?? process.env.BEAM_NATS_URL;
  const environment = parseOptionalString(options.environment) ?? process.env.BEAM_ENV;
  const transferRuntimeShardCount = parseOptionalNumber(options["shard-count"], "--shard-count") ?? (process.env.TRANSFER_RUNTIME_SHARD_COUNT ? Number(process.env.TRANSFER_RUNTIME_SHARD_COUNT) : undefined);

  return new BeamClient({
    apiKey,
    natsUrl,
    environment,
    transferRuntimeShardCount
  });
}

async function waitForTransfer(
  client: BeamClient,
  transferId: string,
  pollIntervalSeconds: number
): Promise<TransferStatusInfo> {
  if (pollIntervalSeconds <= 0) {
    throw new CliError("--poll-interval must be greater than 0.");
  }

  for (;;) {
    const status = await client.transferStatus(transferId);
    if (terminalStates.has(status.status.toLowerCase())) {
      return status;
    }
    console.log(
      `Status: ${status.status} (${status.delivery_tasks_completed}/${status.delivery_tasks_total})`
    );
    await sleep(pollIntervalSeconds * 1000);
  }
}

function parseCommandArgs(args: string[]): { positional: string[]; options: ParsedOptions } {
  const positional: string[] = [];
  const options: ParsedOptions = {};

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (!arg.startsWith("--")) {
      positional.push(arg);
      continue;
    }

    const [rawKey, inlineValue] = arg.slice(2).split("=", 2);
    const key = rawKey.trim();
    if (!key) {
      throw new CliError(`Invalid option: ${arg}`);
    }

    const next = args[index + 1];
    const value =
      inlineValue ??
      (next && !next.startsWith("--")
        ? ((index += 1), next)
        : true);

    const existing = options[key];
    if (existing === undefined) {
      options[key] = value;
    } else if (Array.isArray(existing)) {
      existing.push(String(value));
    } else {
      options[key] = [String(existing), String(value)];
    }
  }

  return { positional, options };
}

function parseOptions(args: string[]): ParsedOptions {
  return parseCommandArgs(args).options;
}

function parseJsonList<T>(value: string | boolean | string[] | undefined, option: string): T[] {
  if (value === undefined || value === true) {
    throw new CliError(`${option} is required.`);
  }

  if (value === false) {
    throw new CliError(`${option} is required.`);
  }

  const values = Array.isArray(value) ? value : [value];
  return values.map((item) => {
    try {
      return JSON.parse(item) as T;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new CliError(`Invalid JSON for ${option}: ${message}`);
    }
  });
}

function parseRequiredNumber(value: string | boolean | string[] | undefined, option: string): number {
  const parsed = parseOptionalNumber(value, option);
  if (parsed === undefined) {
    throw new CliError(`${option} is required.`);
  }
  return parsed;
}

function parseOptionalNumber(value: string | boolean | string[] | undefined, option: string): number | undefined {
  const text = parseOptionalString(value);
  if (text === undefined) {
    return undefined;
  }
  const parsed = Number(text);
  if (!Number.isFinite(parsed)) {
    throw new CliError(`${option} must be a number.`);
  }
  return parsed;
}

function parseOptionalString(value: string | boolean | string[] | undefined): string | undefined {
  if (value === undefined || value === false) {
    return undefined;
  }
  if (value === true) {
    return "true";
  }
  if (Array.isArray(value)) {
    return value[value.length - 1];
  }
  return value;
}

function printStatus(status: TransferStatusInfo): void {
  console.log(`Transfer: ${status.transfer_id}`);
  console.log(`Status:   ${status.status}`);
  console.log(`Size:     ${status.source_bytes_total}`);
  console.log(`Tasks:    ${status.delivery_tasks_completed}/${status.delivery_tasks_total}`);
  if (status.error_message) {
    console.log(`Error:    ${status.error_message}`);
  }
}

function printJson(value: unknown): void {
  console.log(JSON.stringify(value, null, 2));
}

function printHelp(): void {
  console.log(`BEAM CLI

Usage:
  beam-send create --source <json> --destination <json> --total-size <bytes> [options]
  beam-send status <transfer-id> [options]
  beam-send distribute <transfer-id> [options]
  beam-send cancel <transfer-id> [options]

Global options:
  --api-key <key>          Beam API key. Defaults to BEAM_API_KEY.
  --server <url>           NATS URL. Defaults to BEAM_NATS_URL or ${BEAM_DEFAULT_NATS_URL}.
  --environment <env>      Beam environment token. Defaults to BEAM_ENV or prod.
  --shard-count <count>    Runtime shard count. Defaults to TRANSFER_RUNTIME_SHARD_COUNT or 1.
  --json                   Print JSON responses.

Create options:
  --source <json>          Source config. Repeat for multiple sources.
  --destination <json>     Destination config. Repeat for multiple destinations.
  --total-size <bytes>     Total transfer size in bytes.
  --chunk-size <bytes>     Chunk size in bytes.
  --name <name>            Transfer name.
  --distribute             Distribute after creation.
  --wait                   Poll until terminal status.
  --poll-interval <sec>    Poll interval for --wait. Defaults to 2.
  --test-mode              Create transfer in test mode.
  --progressive-mode       Enable progressive transfer mode.

Examples:
  beam-send create \\
    --source '{"type":"http","url":"https://downloads.example.com/report.parquet"}' \\
    --destination '{"type":"http","url":"https://storage.example.com/report.parquet"}' \\
    --total-size 104857600 \\
    --distribute

  beam-send status transfer_123

Default API:
  ${BEAM_DEFAULT_NATS_URL}
`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class CliError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CliError";
  }
}

await main();
