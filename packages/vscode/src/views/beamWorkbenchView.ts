import {
  BEAM_DEFAULT_NATS_URL,
  type DestConfig,
  type ProviderDestinationConfig,
  type ProviderSourceConfig,
  type ProviderTransferCreateInput,
  type RawTransferCreateInput,
  type SourceConfig,
  type TransferStatusInfo
} from "@beam-network/sdk";
import * as vscode from "vscode";
import type { BeamService } from "../services/beamService.js";
import type { CredentialService } from "../services/credentialService.js";
import type { TransferStore } from "../services/transferStore.js";
import type { BeamTransfersProvider } from "./beamTransfersProvider.js";

const terminalStates = new Set(["completed", "failed", "cancelled", "canceled"]);

type ProviderKind = "http" | "s3-compatible" | "r2" | "hippius" | "huggingface";

interface EndpointFormEntry {
  kind: ProviderKind;
  fields: Record<string, string>;
}

interface WorkbenchRunMessage {
  type: "runTransfer";
  endpoint: string;
  apiKey?: string;
  name?: string;
  testMode: boolean;
  sources: EndpointFormEntry[];
  destinations: EndpointFormEntry[];
}

interface SaveSettingsMessage {
  type: "saveSettings";
  endpoint: string;
  apiKey?: string;
}

interface ExportTransferMessage {
  type: "exportTransfer";
  endpoint: string;
  name?: string;
  testMode: boolean;
  sources: EndpointFormEntry[];
  destinations: EndpointFormEntry[];
}

interface TransferConfigFile {
  nats_url: string;
  name?: string;
  test_mode: boolean;
  sources: EndpointFormEntry[];
  destinations: EndpointFormEntry[];
}

type WorkbenchMessage =
  | { type: "ready" }
  | { type: "importTransfer" }
  | { type: "cancelCurrentTransfer" }
  | WorkbenchRunMessage
  | SaveSettingsMessage
  | ExportTransferMessage;

export class BeamWorkbenchViewProvider implements vscode.WebviewViewProvider {
  private view?: vscode.WebviewView;
  private running = false;
  private activeTransfer?: {
    transferId: string;
    apiKey?: string;
    endpoint: string;
  };

  constructor(
    private readonly extensionUri: vscode.Uri,
    private readonly credentials: CredentialService,
    private readonly beam: BeamService,
    private readonly store: TransferStore,
    private readonly transfersProvider: BeamTransfersProvider,
    private readonly output: vscode.OutputChannel
  ) {}

  resolveWebviewView(webviewView: vscode.WebviewView): void {
    this.view = webviewView;
    webviewView.webview.options = {
      enableScripts: true,
      localResourceRoots: [this.extensionUri]
    };
    webviewView.webview.html = this.html(webviewView.webview);

    webviewView.webview.onDidReceiveMessage((message: WorkbenchMessage) => {
      void this.handleMessage(message);
    });
  }

  private async handleMessage(message: WorkbenchMessage): Promise<void> {
    if (message.type === "ready") {
      await this.postInitialState();
      return;
    }

    if (message.type === "saveSettings") {
      await this.saveSettings(message);
      return;
    }

    if (message.type === "exportTransfer") {
      await this.exportTransfer(message);
      return;
    }

    if (message.type === "importTransfer") {
      await this.importTransfer();
      return;
    }

    if (message.type === "cancelCurrentTransfer") {
      await this.cancelCurrentTransfer();
      return;
    }

    if (message.type === "runTransfer") {
      await this.runTransfer(message);
    }
  }

  private async postInitialState(): Promise<void> {
    const endpoint = vscode.workspace
      .getConfiguration("beam")
      .get<string>("endpoint", BEAM_DEFAULT_NATS_URL);
    const apiKey = await this.credentials.getApiKey();
    await this.post({
      type: "init",
      endpoint,
      hasApiKey: Boolean(apiKey?.trim())
    });
  }

  private async saveSettings(message: SaveSettingsMessage): Promise<void> {
    await this.persistSettings(message.endpoint, message.apiKey);
    await this.post({
      type: "saved",
      message: "Settings saved."
    });
  }

  private async exportTransfer(message: ExportTransferMessage): Promise<void> {
    try {
      const config = normalizeTransferConfig({
        nats_url: message.endpoint.trim() || BEAM_DEFAULT_NATS_URL,
        name: message.name?.trim() || undefined,
        test_mode: message.testMode,
        sources: message.sources,
        destinations: message.destinations
      });

      const target = await vscode.window.showSaveDialog({
        defaultUri: vscode.Uri.file("beam-transfer.json"),
        filters: {
          JSON: ["json"]
        },
        saveLabel: "Export Transfer"
      });
      if (!target) {
        return;
      }

      await vscode.workspace.fs.writeFile(
        target,
        new TextEncoder().encode(`${JSON.stringify(config, null, 2)}\n`)
      );
      await this.post({
        type: "saved",
        message: "Transfer exported."
      });
    } catch (error) {
      await this.post({
        type: "error",
        message: this.beam.describeError(error)
      });
    }
  }

  private async importTransfer(): Promise<void> {
    try {
      const selected = await vscode.window.showOpenDialog({
        canSelectFiles: true,
        canSelectFolders: false,
        canSelectMany: false,
        filters: {
          JSON: ["json"]
        },
        openLabel: "Import Transfer"
      });
      const source = selected?.[0];
      if (!source) {
        return;
      }

      const bytes = await vscode.workspace.fs.readFile(source);
      const parsed = JSON.parse(new TextDecoder().decode(bytes)) as unknown;
      const config = normalizeTransferConfig(parsed);

      await this.post({
        type: "imported",
        config,
        message: "Transfer imported."
      });
    } catch (error) {
      await this.post({
        type: "error",
        message: this.beam.describeError(error)
      });
    }
  }

  private async runTransfer(message: WorkbenchRunMessage): Promise<void> {
    if (this.running) {
      await this.post({
        type: "error",
        message: "A transfer is already running from this view."
      });
      return;
    }

    this.running = true;
    await this.post({ type: "running", running: true });

    try {
      await this.persistSettings(message.endpoint, message.apiKey);
      const apiKey = message.apiKey?.trim() || (await this.credentials.getApiKey());
      const endpoint = message.endpoint.trim() || BEAM_DEFAULT_NATS_URL;
      const parsed = await parseRunInput(message);

      await this.postProgress("Creating transfer", 0);

      const transfer =
        parsed.mode === "provider"
          ? await this.beam.createProviderTransfer(parsed.input, { apiKey, endpoint })
          : await this.createAndDistributeRawTransfer(parsed.input, apiKey, endpoint);

      this.activeTransfer = {
        transferId: transfer.transfer_id,
        apiKey,
        endpoint
      };
      await this.post({ type: "canCancel", enabled: true });

      await this.store.upsert({
        transferId: transfer.transfer_id,
        name: message.name?.trim() || undefined,
        status: transfer.success ? "distributing" : "failed",
        totalChunks: transfer.total_chunks,
        chunksCompleted: 0
      });
      this.transfersProvider.refresh();

      this.output.appendLine(`Workbench created transfer: ${transfer.transfer_id}`);
      this.output.appendLine(JSON.stringify(transfer, null, 2));
      await this.post({
        type: "created",
        transferId: transfer.transfer_id,
        totalChunks: transfer.total_chunks
      });

      await this.followTransfer(transfer.transfer_id, { apiKey, endpoint });
    } catch (error) {
      const messageText = this.beam.describeError(error);
      this.output.appendLine(messageText);
      await this.post({
        type: "error",
        message: messageText
      });
    } finally {
      this.running = false;
      this.activeTransfer = undefined;
      await this.post({ type: "running", running: false });
      await this.post({ type: "canCancel", enabled: false });
    }
  }

  private async cancelCurrentTransfer(): Promise<void> {
    if (!this.activeTransfer) {
      await this.post({
        type: "error",
        message: "No running transfer to cancel."
      });
      return;
    }

    try {
      await this.postProgress("Cancelling transfer", 0, this.activeTransfer.transferId);
      const response = await this.beam.cancelTransfer(this.activeTransfer.transferId, {
        apiKey: this.activeTransfer.apiKey,
        endpoint: this.activeTransfer.endpoint
      });
      this.output.appendLine(JSON.stringify(response, null, 2));
      await this.store.upsert({
        transferId: this.activeTransfer.transferId,
        status: response.success ? "cancelled" : undefined
      });
      this.transfersProvider.refresh();
      await this.post({
        type: "cancelled",
        message: response.message ?? "Transfer cancelled."
      });
      this.activeTransfer = undefined;
      await this.post({ type: "canCancel", enabled: false });
    } catch (error) {
      await this.post({
        type: "error",
        message: this.beam.describeError(error)
      });
    }
  }

  private async createAndDistributeRawTransfer(
    input: RawTransferCreateInput,
    apiKey: string | undefined,
    endpoint: string
  ) {
    const transfer = await this.beam.createTransfer(input, { apiKey, endpoint });
    if (transfer.success) {
      await this.postProgress("Distributing transfer", 5, transfer.transfer_id);
      await this.beam.distributeTransfer(transfer.transfer_id, { apiKey, endpoint });
    }
    return transfer;
  }

  private async followTransfer(
    transferId: string,
    overrides: { apiKey?: string; endpoint: string }
  ): Promise<void> {
    const pollIntervalMs = vscode.workspace
      .getConfiguration("beam")
      .get<number>("pollIntervalMs", 5000);

    for (;;) {
      const status = await this.beam.transferStatus(transferId, overrides);
      await this.store.updateStatus(status);
      this.transfersProvider.refresh();
      this.outputStatus(status);

      const percent = progressPercent(status);
      await this.postProgress(status.status, percent, transferId, status);

      if (terminalStates.has(status.status.toLowerCase())) {
        return;
      }

      await sleep(Math.max(1000, pollIntervalMs));
    }
  }

  private async persistSettings(endpoint: string, apiKey: string | undefined): Promise<void> {
    const cleanEndpoint = endpoint.trim();
    if (cleanEndpoint) {
      await vscode.workspace
        .getConfiguration("beam")
        .update("endpoint", cleanEndpoint, vscode.ConfigurationTarget.Global);
    }

    if (apiKey?.trim()) {
      await this.credentials.setApiKey(apiKey.trim());
    }
  }

  private outputStatus(status: TransferStatusInfo): void {
    this.output.appendLine(`Transfer: ${status.transfer_id}`);
    this.output.appendLine(`Status:   ${status.status}`);
    this.output.appendLine(
      `Tasks:    ${status.delivery_tasks_completed}/${status.delivery_tasks_total}`
    );
    if (status.error_message) {
      this.output.appendLine(`Error:    ${status.error_message}`);
    }
  }

  private postProgress(
    label: string,
    percent: number,
    transferId?: string,
    status?: TransferStatusInfo
  ): Thenable<boolean> | undefined {
    return this.post({
      type: "progress",
      label,
      percent,
      transferId,
      status
    });
  }

  private post(message: unknown): Thenable<boolean> | undefined {
    return this.view?.webview.postMessage(message);
  }

  private html(webview: vscode.Webview): string {
    const nonce = createNonce();
    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline' ${webview.cspSource}; script-src 'nonce-${nonce}';">
  <title>BEAM Workbench</title>
  <style>
    :root {
      color-scheme: light dark;
    }

    body {
      padding: 12px;
      color: var(--vscode-foreground);
      background: var(--vscode-sideBar-background);
      font-family: var(--vscode-font-family);
      font-size: var(--vscode-font-size);
    }

    * {
      box-sizing: border-box;
    }

    .stack {
      display: flex;
      flex-direction: column;
      gap: 14px;
    }

    .section {
      border-top: 1px solid var(--vscode-sideBarSectionHeader-border, var(--vscode-panel-border));
      padding-top: 12px;
    }

    .section:first-child {
      border-top: 0;
      padding-top: 0;
    }

    .title {
      margin: 0 0 8px;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0;
      text-transform: uppercase;
      color: var(--vscode-sideBarTitle-foreground);
    }

    label {
      display: block;
      margin: 0 0 4px;
      color: var(--vscode-descriptionForeground);
    }

    input,
    select {
      width: 100%;
      border: 1px solid var(--vscode-input-border, transparent);
      border-radius: 3px;
      padding: 7px;
      color: var(--vscode-input-foreground);
      background: var(--vscode-input-background);
      font-family: var(--vscode-font-family);
      font-size: var(--vscode-font-size);
    }

    input:focus,
    select:focus {
      outline: 1px solid var(--vscode-focusBorder);
      outline-offset: -1px;
    }

    .field {
      margin-bottom: 10px;
    }

    .toggle {
      display: flex;
      align-items: center;
      gap: 8px;
      margin: 8px 0;
      color: var(--vscode-foreground);
    }

    .toggle input {
      width: auto;
    }

    button {
      min-height: 28px;
      border: 1px solid var(--vscode-button-border, transparent);
      border-radius: 3px;
      padding: 5px 10px;
      color: var(--vscode-button-foreground);
      background: var(--vscode-button-background);
      font: inherit;
      cursor: pointer;
    }

    button.secondary {
      color: var(--vscode-button-secondaryForeground);
      background: var(--vscode-button-secondaryBackground);
    }

    button.icon {
      flex: 0 0 auto;
      width: 30px;
      padding: 0;
      font-weight: 700;
    }

    button:disabled {
      opacity: 0.55;
      cursor: default;
    }

    .endpoint {
      border: 1px solid var(--vscode-panel-border);
      border-radius: 4px;
      padding: 8px;
      margin-bottom: 8px;
      background: var(--vscode-sideBar-background);
    }

    .endpoint-head {
      display: flex;
      gap: 6px;
      align-items: center;
      margin-bottom: 8px;
    }

    .endpoint-head select {
      flex: 1;
    }

    .grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
    }

    .progress-shell {
      width: 100%;
      height: 10px;
      overflow: hidden;
      border-radius: 999px;
      background: var(--vscode-progressBar-background);
      border: 1px solid var(--vscode-panel-border);
    }

    .progress-fill {
      width: 0%;
      height: 100%;
      background: var(--vscode-charts-green);
      transition: width 180ms ease;
    }

    .progress-fill.running {
      background: var(--vscode-progressBar-background);
    }

    .progress-fill.completed {
      background: var(--vscode-charts-green);
    }

    .progress-fill.failed {
      background: var(--vscode-charts-red);
    }

    .progress-fill.cancelled {
      background: var(--vscode-charts-yellow);
    }

    .status {
      min-height: 20px;
      margin-top: 8px;
      color: var(--vscode-descriptionForeground);
      overflow-wrap: anywhere;
    }

    .error {
      color: var(--vscode-errorForeground);
    }

    .transfer-id {
      font-family: var(--vscode-editor-font-family);
      color: var(--vscode-textLink-foreground);
      overflow-wrap: anywhere;
    }
  </style>
</head>
<body>
  <div class="stack">
    <section class="section">
      <h2 class="title">Connection</h2>
      <div class="field">
        <label for="endpoint">Beam NATS URL</label>
        <input id="endpoint" type="url" placeholder="${BEAM_DEFAULT_NATS_URL}">
      </div>
      <div class="field">
        <label for="apiKey">API Key</label>
        <input id="apiKey" type="password" placeholder="No API key saved">
      </div>
      <button id="saveSettings" type="button" class="secondary">Save</button>
    </section>

    <section class="section">
      <h2 class="title">Transfer</h2>
      <div class="field">
        <label for="name">Name</label>
        <input id="name" type="text" placeholder="daily-report">
      </div>
      <label class="toggle"><input id="testMode" type="checkbox"> Test mode</label>
      <div class="endpoint-head">
        <button id="importTransfer" type="button" class="secondary">Import JSON</button>
        <button id="exportTransfer" type="button" class="secondary">Export JSON</button>
      </div>
    </section>

    <section class="section">
      <h2 class="title">Sources</h2>
      <div id="sources"></div>
      <button id="addSource" type="button" class="secondary">Add source</button>
    </section>

    <section class="section">
      <h2 class="title">Destinations</h2>
      <div id="destinations"></div>
      <button id="addDestination" type="button" class="secondary">Add destination</button>
    </section>

    <section class="section">
      <div class="endpoint-head">
        <button id="runTransfer" type="button">Run Transfer</button>
        <button id="cancelTransfer" type="button" class="secondary" disabled>Cancel Transfer</button>
      </div>
      <div class="status" id="transferId"></div>
      <div class="progress-shell" aria-label="Transfer progress">
        <div id="progressFill" class="progress-fill"></div>
      </div>
      <div class="status" id="status">Idle</div>
    </section>
  </div>

  <script nonce="${nonce}">
    const vscode = acquireVsCodeApi();
    const endpoint = document.getElementById("endpoint");
    const apiKey = document.getElementById("apiKey");
    const nameInput = document.getElementById("name");
    const testMode = document.getElementById("testMode");
    const sourcesRoot = document.getElementById("sources");
    const destinationsRoot = document.getElementById("destinations");
    const runTransfer = document.getElementById("runTransfer");
    const cancelTransfer = document.getElementById("cancelTransfer");
    const saveSettings = document.getElementById("saveSettings");
    const importTransfer = document.getElementById("importTransfer");
    const exportTransfer = document.getElementById("exportTransfer");
    const progressFill = document.getElementById("progressFill");
    const statusNode = document.getElementById("status");
    const transferIdNode = document.getElementById("transferId");

    const providerOptions = [
      ["http", "HTTP"],
      ["s3-compatible", "S3-compatible"],
      ["r2", "R2"],
      ["hippius", "Hippius"],
      ["huggingface", "Hugging Face"]
    ];

    const fieldSchemas = {
      "http": [
        ["url", "URL", "https://downloads.example.com/report.parquet", "url"],
        ["headers", "Headers JSON", "{\\"Authorization\\":\\"Bearer ...\\"}", "text"]
      ],
      "s3-compatible": [
        ["provider", "Provider", "s3", "text"],
        ["bucket", "Bucket", "beam-bucket", "text"],
        ["key", "Key", "path/report.parquet", "text"],
        ["region", "Region", "us-east-1", "text"],
        ["endpoint_url", "Endpoint URL", "https://s3.example.com", "url"],
        ["access_key_id", "Access key ID", "", "password"],
        ["secret_access_key", "Secret access key", "", "password"],
        ["session_token", "Session token", "", "password"],
        ["force_path_style", "Force path style", "false", "text"]
      ],
      "r2": [
        ["bucket", "Bucket", "beam-bucket", "text"],
        ["key", "Key", "path/report.parquet", "text"],
        ["account_id", "Account ID", "", "text"],
        ["endpoint_url", "Endpoint URL", "https://account.r2.cloudflarestorage.com", "url"],
        ["access_key_id", "Access key ID", "", "password"],
        ["secret_access_key", "Secret access key", "", "password"]
      ],
      "hippius": [
        ["bucket", "Bucket", "beam-bucket", "text"],
        ["key", "Key", "path/report.parquet", "text"],
        ["api_token", "API token", "", "password"],
        ["base_url", "Base URL", "https://api.hippius.com", "url"]
      ],
      "huggingface": [
        ["repo_id", "Repo ID", "org/dataset", "text"],
        ["path", "Path in repo", "data/train.parquet", "text"],
        ["repo_type", "Repo type", "dataset", "text"],
        ["revision", "Revision", "main", "text"],
        ["token", "Access token", "", "password"],
        ["endpoint", "Hub endpoint", "https://huggingface.co", "url"]
      ]
    };

    let sources = [defaultEntry("source")];
    let destinations = [defaultEntry("destination")];

    document.getElementById("addSource").addEventListener("click", () => {
      sources.push(defaultEntry("source"));
      renderEndpointList("source");
    });

    document.getElementById("addDestination").addEventListener("click", () => {
      destinations.push(defaultEntry("destination"));
      renderEndpointList("destination");
    });

    saveSettings.addEventListener("click", () => {
      vscode.postMessage({
        type: "saveSettings",
        endpoint: endpoint.value,
        apiKey: apiKey.value
      });
    });

    importTransfer.addEventListener("click", () => {
      vscode.postMessage({ type: "importTransfer" });
    });

    exportTransfer.addEventListener("click", () => {
      vscode.postMessage({
        type: "exportTransfer",
        endpoint: endpoint.value,
        name: nameInput.value,
        testMode: testMode.checked,
        sources,
        destinations
      });
    });

    runTransfer.addEventListener("click", () => {
      setStatus("Starting transfer...");
      setProgress(0);
      setProgressState("running");
      transferIdNode.textContent = "";
      vscode.postMessage({
        type: "runTransfer",
        endpoint: endpoint.value,
        apiKey: apiKey.value,
        name: nameInput.value,
        testMode: testMode.checked,
        sources,
        destinations
      });
    });

    cancelTransfer.addEventListener("click", () => {
      cancelTransfer.disabled = true;
      setStatus("Cancelling transfer...");
      setProgressState("cancelled");
      vscode.postMessage({ type: "cancelCurrentTransfer" });
    });

    window.addEventListener("message", (event) => {
      const message = event.data;
      if (message.type === "init") {
        endpoint.value = message.endpoint || "";
        apiKey.placeholder = message.hasApiKey ? "Saved API key configured" : "No API key saved";
        renderEndpointList("source");
        renderEndpointList("destination");
        return;
      }

      if (message.type === "saved") {
        apiKey.value = "";
        apiKey.placeholder = "Saved API key configured";
        setStatus(message.message);
        return;
      }

      if (message.type === "imported") {
        endpoint.value = message.config.nats_url || "";
        nameInput.value = message.config.name || "";
        testMode.checked = Boolean(message.config.test_mode);
        sources = message.config.sources;
        destinations = message.config.destinations;
        renderEndpointList("source");
        renderEndpointList("destination");
        setStatus(message.message);
        return;
      }

      if (message.type === "running") {
        setRunning(Boolean(message.running));
        return;
      }

      if (message.type === "canCancel") {
        cancelTransfer.disabled = !message.enabled;
        return;
      }

      if (message.type === "cancelled") {
        setProgressState("cancelled");
        setStatus(message.message);
        return;
      }

      if (message.type === "created") {
        transferIdNode.innerHTML = '<span class="transfer-id">' + escapeHtml(message.transferId) + "</span>";
        setStatus("Created transfer" + (message.totalChunks ? " with " + message.totalChunks + " chunks." : "."));
        return;
      }

      if (message.type === "progress") {
        setProgress(message.percent || 0);
        setProgressState(message.status ? message.status.status : message.label);
        if (message.transferId) {
          transferIdNode.innerHTML = '<span class="transfer-id">' + escapeHtml(message.transferId) + "</span>";
        }
        const chunks = message.status && message.status.total_chunks
          ? " (" + (message.status.chunks_completed || 0) + "/" + message.status.total_chunks + ")"
          : "";
        setStatus((message.label || "Running") + chunks);
        return;
      }

      if (message.type === "error") {
        setProgressState("failed");
        setStatus(message.message, true);
      }
    });

    function defaultEntry(role) {
      return {
        kind: "http",
        fields: {
          url: role === "source"
            ? "https://downloads.example.com/report.parquet"
            : "https://storage.example.com/ingest/report.parquet",
          headers: ""
        }
      };
    }

    function renderEndpointList(role) {
      const values = role === "source" ? sources : destinations;
      const root = role === "source" ? sourcesRoot : destinationsRoot;
      root.innerHTML = "";

      values.forEach((entry, index) => {
        const wrapper = document.createElement("div");
        wrapper.className = "endpoint";

        const head = document.createElement("div");
        head.className = "endpoint-head";

        const select = document.createElement("select");
        providerOptions.forEach(([value, label]) => {
          const option = document.createElement("option");
          option.value = value;
          option.textContent = label;
          option.selected = entry.kind === value;
          select.append(option);
        });
        select.addEventListener("change", () => {
          entry.kind = select.value;
          entry.fields = defaultsFor(entry.kind, role);
          renderEndpointList(role);
        });

        const remove = document.createElement("button");
        remove.type = "button";
        remove.className = "icon secondary";
        remove.textContent = "-";
        remove.title = "Remove";
        remove.disabled = values.length === 1;
        remove.addEventListener("click", () => {
          values.splice(index, 1);
          renderEndpointList(role);
        });

        head.append(select, remove);
        wrapper.append(head, renderFields(entry));
        root.append(wrapper);
      });
    }

    function renderFields(entry) {
      const grid = document.createElement("div");
      grid.className = "grid";
      const schema = fieldSchemas[entry.kind] || fieldSchemas.http;

      schema.forEach(([key, label, placeholder, type]) => {
        const field = document.createElement("div");
        field.className = "field";

        const labelNode = document.createElement("label");
        labelNode.textContent = label;

        const input = document.createElement("input");
        input.type = type;
        input.placeholder = placeholder;
        input.value = entry.fields[key] || "";
        input.addEventListener("input", () => {
          entry.fields[key] = input.value;
        });

        field.append(labelNode, input);
        grid.append(field);
      });

      return grid;
    }

    function defaultsFor(kind, role) {
      if (kind === "http") {
        return defaultEntry(role).fields;
      }
      if (kind === "s3-compatible") {
        return { provider: "s3", bucket: "", key: "", region: "", endpoint_url: "", access_key_id: "", secret_access_key: "", session_token: "", force_path_style: "" };
      }
      if (kind === "r2") {
        return { bucket: "", key: "", account_id: "", endpoint_url: "", access_key_id: "", secret_access_key: "" };
      }
      if (kind === "huggingface") {
        return { repo_id: "", path: "", repo_type: "dataset", revision: "main", token: "", endpoint: "https://huggingface.co" };
      }
      return { bucket: "", key: "", api_token: "", base_url: "https://api.hippius.com" };
    }

    function setRunning(running) {
      runTransfer.disabled = running;
      saveSettings.disabled = running;
      importTransfer.disabled = running;
      exportTransfer.disabled = running;
    }

    function setProgress(percent) {
      progressFill.style.width = Math.max(0, Math.min(100, percent)) + "%";
    }

    function setProgressState(status) {
      const normalized = String(status || "").toLowerCase();
      progressFill.classList.remove("running", "completed", "failed", "cancelled");
      if (normalized === "completed") {
        progressFill.classList.add("completed");
        return;
      }
      if (normalized === "failed") {
        progressFill.classList.add("failed");
        return;
      }
      if (normalized === "cancelled" || normalized === "canceled") {
        progressFill.classList.add("cancelled");
        return;
      }
      progressFill.classList.add("running");
    }

    function setStatus(text, isError) {
      statusNode.textContent = text;
      statusNode.classList.toggle("error", Boolean(isError));
    }

    function escapeHtml(value) {
      return String(value).replace(/[&<>"']/g, (char) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      }[char]));
    }

    vscode.postMessage({ type: "ready" });
  </script>
</body>
</html>`;
  }
}

type ParsedWorkbenchInput =
  | { mode: "raw"; input: RawTransferCreateInput }
  | { mode: "provider"; input: ProviderTransferCreateInput };

async function parseRunInput(message: WorkbenchRunMessage): Promise<ParsedWorkbenchInput> {
  const sourceKinds = new Set(message.sources.map((source) => source.kind));
  const destinationKinds = new Set(message.destinations.map((destination) => destination.kind));
  const hasHttp = sourceKinds.has("http") || destinationKinds.has("http");
  const hasProvider =
    [...sourceKinds, ...destinationKinds].some((kind) => kind !== "http");

  if (hasHttp && hasProvider) {
    throw new Error("Mixed HTTP and provider transfers are not supported in the sidebar yet.");
  }

  if (hasProvider) {
    return {
      mode: "provider",
      input: {
        sources: message.sources.map((source, index) => providerSource(source, index)),
        destinations: message.destinations.map((destination, index) =>
          providerDestination(destination, index)
        ),
        name: message.name?.trim() || undefined,
        testMode: message.testMode,
        distribute: true
      }
    };
  }

  const sources = message.sources.map((source, index) => httpSource(source, index));
  return {
    mode: "raw",
    input: {
      sources,
      destinations: message.destinations.map((destination, index) => httpDestination(destination, index)),
      totalSize: await inferHttpTotalSize(sources),
      name: message.name?.trim() || undefined,
      testMode: message.testMode
    }
  };
}

function httpSource(entry: EndpointFormEntry, index: number): SourceConfig {
  if (entry.kind !== "http") {
    throw new Error(`Source ${index + 1} must be HTTP.`);
  }
  const url = required(entry, "url", `Source ${index + 1}`);
  return compact({
    type: "http",
    url,
    headers: optionalHeaders(entry, `Source ${index + 1}`)
  });
}

function httpDestination(entry: EndpointFormEntry, index: number): DestConfig {
  if (entry.kind !== "http") {
    throw new Error(`Destination ${index + 1} must be HTTP.`);
  }
  const url = required(entry, "url", `Destination ${index + 1}`);
  return compact({
    type: "http",
    url,
    headers: optionalHeaders(entry, `Destination ${index + 1}`)
  });
}

function providerSource(entry: EndpointFormEntry, index: number): ProviderSourceConfig {
  return providerConfig(entry, `Source ${index + 1}`) as ProviderSourceConfig;
}

function providerDestination(entry: EndpointFormEntry, index: number): ProviderDestinationConfig {
  return providerConfig(entry, `Destination ${index + 1}`) as ProviderDestinationConfig;
}

function providerConfig(entry: EndpointFormEntry, label: string): ProviderSourceConfig {
  if (entry.kind === "http") {
    throw new Error(`${label} must use a provider type.`);
  }

  if (entry.kind === "s3-compatible") {
    const provider = required(entry, "provider", label).trim().toLowerCase();
    return compact({
      provider,
      driver: "s3-compatible" as const,
      bucket: required(entry, "bucket", label),
      key: required(entry, "key", label),
      region: optional(entry, "region"),
      endpoint_url: optional(entry, "endpoint_url"),
      access_key_id: required(entry, "access_key_id", label),
      secret_access_key: required(entry, "secret_access_key", label),
      session_token: optional(entry, "session_token"),
      force_path_style: optionalBoolean(entry, "force_path_style")
    }) as ProviderSourceConfig;
  }

  if (entry.kind === "r2") {
    return compact({
      provider: "r2" as const,
      bucket: required(entry, "bucket", label),
      key: required(entry, "key", label),
      account_id: optional(entry, "account_id"),
      endpoint_url: optional(entry, "endpoint_url"),
      access_key_id: required(entry, "access_key_id", label),
      secret_access_key: required(entry, "secret_access_key", label)
    }) as ProviderSourceConfig;
  }

  if (entry.kind === "huggingface") {
    return compact({
      provider: "huggingface" as const,
      repo_id: required(entry, "repo_id", label),
      path: required(entry, "path", label),
      repo_type: optional(entry, "repo_type"),
      revision: optional(entry, "revision"),
      token: required(entry, "token", label),
      endpoint: optional(entry, "endpoint")
    }) as ProviderSourceConfig;
  }

  return compact({
    provider: "hippius" as const,
    bucket: required(entry, "bucket", label),
    key: required(entry, "key", label),
    api_token: required(entry, "api_token", label),
    base_url: optional(entry, "base_url")
  }) as ProviderSourceConfig;
}

async function inferHttpTotalSize(sources: SourceConfig[]): Promise<number> {
  let total = 0;
  for (const [index, source] of sources.entries()) {
    if (!source.url) {
      throw new Error(`Source ${index + 1}: URL is required.`);
    }

    const response = await fetch(source.url, {
      method: "HEAD",
      headers: source.headers
    });
    if (!response.ok) {
      throw new Error(`Source ${index + 1}: HEAD request failed with ${response.status}.`);
    }

    const contentLength = response.headers.get("content-length");
    const size = contentLength ? Number(contentLength) : Number.NaN;
    if (!Number.isFinite(size) || size <= 0) {
      throw new Error(`Source ${index + 1}: could not infer content length from HTTP headers.`);
    }
    total += size;
  }
  return total;
}

function required(entry: EndpointFormEntry, key: string, label: string): string {
  const value = optional(entry, key);
  if (!value) {
    throw new Error(`${label}: ${key} is required.`);
  }
  return value;
}

function optional(entry: EndpointFormEntry, key: string): string | undefined {
  const value = entry.fields[key]?.trim();
  return value || undefined;
}

function optionalBoolean(entry: EndpointFormEntry, key: string): boolean | undefined {
  const value = optional(entry, key);
  if (!value) {
    return undefined;
  }
  if (["true", "1", "yes"].includes(value.toLowerCase())) {
    return true;
  }
  if (["false", "0", "no"].includes(value.toLowerCase())) {
    return false;
  }
  throw new Error(`${key} must be true or false.`);
}

function optionalHeaders(entry: EndpointFormEntry, label: string): Record<string, string> | undefined {
  const value = optional(entry, "headers");
  if (!value) {
    return undefined;
  }
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("headers must be a JSON object.");
    }
    return parsed as Record<string, string>;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`${label}: ${message}`);
  }
}

function compact<T extends Record<string, unknown>>(input: T): T {
  return Object.fromEntries(
    Object.entries(input).filter(([, value]) => value !== undefined)
  ) as T;
}

function progressPercent(status: TransferStatusInfo): number {
  if (!status.delivery_tasks_total || status.delivery_tasks_total <= 0) {
    return terminalStates.has(status.status.toLowerCase()) ? 100 : 0;
  }
  return Math.round(
    (status.delivery_tasks_completed / status.delivery_tasks_total) * 100
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createNonce(): string {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let value = "";
  for (let index = 0; index < 32; index += 1) {
    value += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return value;
}

function normalizeTransferConfig(value: unknown): TransferConfigFile {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Transfer config must be a JSON object.");
  }

  const input = value as Partial<TransferConfigFile>;
  const sources = normalizeEntries(input.sources, "sources");
  const destinations = normalizeEntries(input.destinations, "destinations");

  return {
    nats_url:
      typeof input.nats_url === "string" && input.nats_url.trim()
        ? input.nats_url.trim()
        : BEAM_DEFAULT_NATS_URL,
    name: typeof input.name === "string" && input.name.trim() ? input.name.trim() : undefined,
    test_mode: Boolean(input.test_mode),
    sources,
    destinations
  };
}

function normalizeEntries(value: unknown, label: string): EndpointFormEntry[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error(`${label} must be a non-empty array.`);
  }

  return value.map((entry, index) => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error(`${label}[${index}] must be an object.`);
    }

    const candidate = entry as Partial<EndpointFormEntry>;
    if (
      candidate.kind !== "http" &&
      candidate.kind !== "s3-compatible" &&
      candidate.kind !== "r2" &&
      candidate.kind !== "hippius" &&
      candidate.kind !== "huggingface"
    ) {
      throw new Error(`${label}[${index}].kind is invalid.`);
    }

    if (!candidate.fields || typeof candidate.fields !== "object" || Array.isArray(candidate.fields)) {
      throw new Error(`${label}[${index}].fields must be an object.`);
    }

    return {
      kind: candidate.kind,
      fields: Object.fromEntries(
        Object.entries(candidate.fields).map(([key, fieldValue]) => [
          key,
          fieldValue === undefined || fieldValue === null ? "" : String(fieldValue)
        ])
      )
    };
  });
}
