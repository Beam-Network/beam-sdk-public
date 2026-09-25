import {
  BEAM_DEFAULT_NATS_URL,
  BeamApiError,
  BeamClient,
  type DestConfig,
  type ProviderTransferCreateInput,
  type RawTransferCreateInput,
  type SourceConfig
} from "@beam-network/sdk";
import * as vscode from "vscode";
import type { CredentialService } from "./credentialService.js";

export interface BeamClientOverrides {
  apiKey?: string;
  endpoint?: string;
}

export interface CreateTransferPromptInput {
  sources: SourceConfig[];
  destinations: DestConfig[];
  totalSize: number;
  chunkSize?: number;
  name?: string;
  testMode?: boolean;
  progressiveMode?: boolean;
}

export class BeamService {
  constructor(
    private readonly credentials: CredentialService,
    private readonly output: vscode.OutputChannel
  ) {}

  async createTransfer(input: RawTransferCreateInput, overrides: BeamClientOverrides = {}) {
    return this.client(overrides).then((client) => client.createRawTransfer(input));
  }

  async createProviderTransfer(input: ProviderTransferCreateInput, overrides: BeamClientOverrides = {}) {
    return this.client(overrides).then((client) => client.createTransfer(input));
  }

  async transferStatus(transferId: string, overrides: BeamClientOverrides = {}) {
    return this.client(overrides).then((client) => client.transferStatus(transferId));
  }

  async distributeTransfer(transferId: string, overrides: BeamClientOverrides = {}) {
    return this.client(overrides).then((client) => client.distributeTransfer(transferId));
  }

  async cancelTransfer(transferId: string, overrides: BeamClientOverrides = {}) {
    return this.client(overrides).then((client) => client.cancelTransfer(transferId));
  }

  describeError(error: unknown): string {
    if (error instanceof BeamApiError) {
      return `BeamCore API error status=${error.status}`;
    }
    if (error instanceof Error) {
      const record = error as Error & { code?: unknown; status?: unknown; statusCode?: unknown };
      const code = typeof record.code === "string" && /^[A-Za-z0-9_.-]{1,64}$/.test(record.code)
        ? record.code
        : error.name;
      const status = [record.status, record.statusCode]
        .find((value): value is number => Number.isInteger(value) && Number(value) >= 100 && Number(value) <= 599);
      return status === undefined ? code : `${code} status=${status}`;
    }
    return "UnknownError";
  }

  private async client(overrides: BeamClientOverrides = {}): Promise<BeamClient> {
    const apiKey = overrides.apiKey ?? (await this.credentials.getApiKey());
    if (!apiKey?.trim()) {
      throw new Error("No BEAM API key configured. Run Beam: Manage API Key first.");
    }

    const endpoint =
      overrides.endpoint ||
      vscode.workspace.getConfiguration("beam").get<string>("endpoint", BEAM_DEFAULT_NATS_URL);

    this.output.appendLine(`Using Beam NATS endpoint: ${endpoint}`);
    return new BeamClient({
      apiKey,
      natsUrl: endpoint
    });
  }
}
