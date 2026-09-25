import type { DestConfig, SourceConfig, TransferStatusInfo } from "@beam-network/sdk";
import * as vscode from "vscode";
import { BeamService } from "./services/beamService.js";
import { CredentialService } from "./services/credentialService.js";
import { TransferStore } from "./services/transferStore.js";
import { BeamWorkbenchViewProvider } from "./views/beamWorkbenchView.js";
import { BeamTransfersProvider, TransferTreeItem } from "./views/beamTransfersProvider.js";

export function activate(context: vscode.ExtensionContext): void {
  const output = vscode.window.createOutputChannel("Beam");
  const credentials = new CredentialService(context.secrets);
  const beam = new BeamService(credentials, output);
  const store = new TransferStore(context.globalState);
  const transfersProvider = new BeamTransfersProvider(store);
  const workbenchProvider = new BeamWorkbenchViewProvider(
    context.extensionUri,
    credentials,
    beam,
    store,
    transfersProvider,
    output
  );

  context.subscriptions.push(
    output,
    vscode.window.registerWebviewViewProvider("beam.workbench", workbenchProvider),
    vscode.window.registerTreeDataProvider("beam.transfers", transfersProvider),
    vscode.commands.registerCommand("beam.manageApiKey", () => manageApiKey(credentials)),
    vscode.commands.registerCommand("beam.clearApiKey", () => clearApiKey(credentials)),
    vscode.commands.registerCommand("beam.createTransfer", () =>
      createTransfer(beam, store, transfersProvider, output)
    ),
    vscode.commands.registerCommand("beam.checkStatus", (item?: TransferTreeItem) =>
      checkStatus(beam, store, transfersProvider, output, item)
    ),
    vscode.commands.registerCommand("beam.distributeTransfer", (item?: TransferTreeItem) =>
      distributeTransfer(beam, store, transfersProvider, output, item)
    ),
    vscode.commands.registerCommand("beam.cancelTransfer", (item?: TransferTreeItem) =>
      cancelTransfer(beam, store, transfersProvider, output, item)
    ),
    vscode.commands.registerCommand("beam.refreshTransfers", () =>
      refreshTransfers(beam, store, transfersProvider, output)
    ),
    vscode.commands.registerCommand("beam.copyTransferId", (item?: TransferTreeItem) =>
      copyTransferId(item)
    ),
    vscode.commands.registerCommand("beam.openTransferJson", (item?: TransferTreeItem) =>
      openTransferJson(item)
    )
  );
}

export function deactivate(): void {}

async function manageApiKey(credentials: CredentialService): Promise<void> {
  const apiKey = await vscode.window.showInputBox({
    title: "BEAM API Key",
    prompt: "Enter your BEAM API key.",
    password: true,
    ignoreFocusOut: true,
    validateInput: (value) => (value.trim() ? undefined : "API key is required.")
  });

  if (apiKey === undefined) {
    return;
  }

  await credentials.setApiKey(apiKey.trim());
  vscode.window.showInformationMessage("BEAM API key saved.");
}

async function clearApiKey(credentials: CredentialService): Promise<void> {
  await credentials.clearApiKey();
  vscode.window.showInformationMessage("BEAM API key cleared.");
}

async function createTransfer(
  beam: BeamService,
  store: TransferStore,
  provider: BeamTransfersProvider,
  output: vscode.OutputChannel
): Promise<void> {
  const input = await promptForTransferInput();
  if (!input) {
    return;
  }

  const distribute = await confirm("Distribute transfer after creation?");

  await runBeamCommand("Creating BEAM transfer", beam, output, async () => {
    const transfer = await beam.createTransfer(input);
    await store.upsert({
      transferId: transfer.transfer_id,
      status: transfer.success ? "created" : "failed",
      totalChunks: transfer.total_chunks,
      name: input.name
    });

    output.appendLine(`Created transfer: ${transfer.transfer_id}`);
    output.appendLine(JSON.stringify(transfer, null, 2));

    if (distribute) {
      const distributed = await beam.distributeTransfer(transfer.transfer_id);
      output.appendLine(`Distributed transfer: ${distributed.transfer_id}`);
      output.appendLine(JSON.stringify(distributed, null, 2));
      await store.upsert({
        transferId: transfer.transfer_id,
        status: distributed.success ? "distributing" : "created",
        totalChunks: transfer.total_chunks,
        name: input.name
      });
    }

    provider.refresh();
    vscode.window.showInformationMessage(`BEAM transfer created: ${transfer.transfer_id}`);
  });
}

async function checkStatus(
  beam: BeamService,
  store: TransferStore,
  provider: BeamTransfersProvider,
  output: vscode.OutputChannel,
  item?: TransferTreeItem
): Promise<void> {
  const transferId = await getTransferId(item);
  if (!transferId) {
    return;
  }

  await runBeamCommand(`Checking ${transferId}`, beam, output, async () => {
    const status = await beam.transferStatus(transferId);
    await store.updateStatus(status);
    provider.refresh();
    printStatus(output, status);
    vscode.window.showInformationMessage(
      `BEAM ${status.transfer_id}: ${status.status} (${status.delivery_tasks_completed}/${status.delivery_tasks_total})`
    );
  });
}

async function distributeTransfer(
  beam: BeamService,
  store: TransferStore,
  provider: BeamTransfersProvider,
  output: vscode.OutputChannel,
  item?: TransferTreeItem
): Promise<void> {
  const transferId = await getTransferId(item);
  if (!transferId) {
    return;
  }

  await runBeamCommand(`Distributing ${transferId}`, beam, output, async () => {
    const response = await beam.distributeTransfer(transferId);
    output.appendLine(JSON.stringify(response, null, 2));
    await store.upsert({
      transferId,
      status: response.success ? "distributing" : undefined
    });
    provider.refresh();
    vscode.window.showInformationMessage(`BEAM transfer distributed: ${transferId}`);
  });
}

async function cancelTransfer(
  beam: BeamService,
  store: TransferStore,
  provider: BeamTransfersProvider,
  output: vscode.OutputChannel,
  item?: TransferTreeItem
): Promise<void> {
  const transferId = await getTransferId(item);
  if (!transferId) {
    return;
  }

  const shouldCancel = await confirm(`Cancel ${transferId}?`);
  if (!shouldCancel) {
    return;
  }

  await runBeamCommand(`Cancelling ${transferId}`, beam, output, async () => {
    const response = await beam.cancelTransfer(transferId);
    output.appendLine(JSON.stringify(response, null, 2));
    await store.upsert({
      transferId,
      status: response.success ? "cancelled" : undefined
    });
    provider.refresh();
    vscode.window.showInformationMessage(response.message ?? `BEAM transfer cancelled: ${transferId}`);
  });
}

async function refreshTransfers(
  beam: BeamService,
  store: TransferStore,
  provider: BeamTransfersProvider,
  output: vscode.OutputChannel
): Promise<void> {
  const transfers = store.getAll();
  if (transfers.length === 0) {
    provider.refresh();
    vscode.window.showInformationMessage("No recent BEAM transfers yet.");
    return;
  }

  await runBeamCommand("Refreshing BEAM transfers", beam, output, async () => {
    for (const transfer of transfers) {
      try {
        const status = await beam.transferStatus(transfer.transferId);
        await store.updateStatus(status);
        printStatus(output, status);
      } catch (error) {
        output.appendLine(`Failed to refresh ${transfer.transferId}: ${beam.describeError(error)}`);
      }
    }
    provider.refresh();
  });
}

async function copyTransferId(item?: TransferTreeItem): Promise<void> {
  const transferId = item?.transfer.transferId;
  if (!transferId) {
    vscode.window.showWarningMessage("Select a BEAM transfer first.");
    return;
  }

  await vscode.env.clipboard.writeText(transferId);
  vscode.window.showInformationMessage(`Copied BEAM transfer ID: ${transferId}`);
}

async function openTransferJson(item?: TransferTreeItem): Promise<void> {
  if (!item?.transfer.rawStatus) {
    vscode.window.showWarningMessage("Refresh this BEAM transfer before opening JSON.");
    return;
  }

  const document = await vscode.workspace.openTextDocument({
    content: JSON.stringify(item.transfer.rawStatus, null, 2),
    language: "json"
  });
  await vscode.window.showTextDocument(document, { preview: true });
}

async function promptForTransferInput(): Promise<{
  sources: SourceConfig[];
  destinations: DestConfig[];
  totalSize: number;
  chunkSize?: number;
  name?: string;
  testMode?: boolean;
  progressiveMode?: boolean;
} | undefined> {
  const source = await promptJson<SourceConfig>(
    "Source JSON",
    '{"type":"http","url":"https://downloads.example.com/report.parquet"}'
  );
  if (!source) {
    return undefined;
  }

  const destination = await promptJson<DestConfig>(
    "Destination JSON",
    '{"type":"http","url":"https://storage.example.com/ingest/report.parquet"}'
  );
  if (!destination) {
    return undefined;
  }

  const totalSizeText = await vscode.window.showInputBox({
    title: "Total Size",
    prompt: "Total transfer size in bytes.",
    placeHolder: "104857600",
    ignoreFocusOut: true,
    validateInput: validatePositiveNumber
  });
  if (totalSizeText === undefined) {
    return undefined;
  }

  const chunkSizeText = await vscode.window.showInputBox({
    title: "Chunk Size",
    prompt: "Optional chunk size in bytes. Leave empty to use the SDK default.",
    ignoreFocusOut: true,
    validateInput: (value) => (value.trim() ? validatePositiveNumber(value) : undefined)
  });
  if (chunkSizeText === undefined) {
    return undefined;
  }

  const name = await vscode.window.showInputBox({
    title: "Transfer Name",
    prompt: "Optional display name.",
    ignoreFocusOut: true
  });
  if (name === undefined) {
    return undefined;
  }

  const testMode = await confirm("Create transfer in test mode?");
  const progressiveMode = await confirm("Enable progressive mode?");

  return {
    sources: [source],
    destinations: [destination],
    totalSize: Number(totalSizeText),
    chunkSize: chunkSizeText.trim() ? Number(chunkSizeText) : undefined,
    name: name.trim() || undefined,
    testMode,
    progressiveMode
  };
}

async function promptJson<T>(title: string, placeHolder: string): Promise<T | undefined> {
  const value = await vscode.window.showInputBox({
    title,
    prompt: "Paste a single JSON object.",
    placeHolder,
    ignoreFocusOut: true,
    validateInput: (text) => {
      try {
        const parsed = JSON.parse(text) as unknown;
        return parsed && typeof parsed === "object" && !Array.isArray(parsed)
          ? undefined
          : "Value must be a JSON object.";
      } catch (error) {
        return error instanceof Error ? error.message : String(error);
      }
    }
  });

  if (value === undefined) {
    return undefined;
  }

  return JSON.parse(value) as T;
}

async function getTransferId(item?: TransferTreeItem): Promise<string | undefined> {
  if (item?.transfer.transferId) {
    return item.transfer.transferId;
  }

  return vscode.window.showInputBox({
    title: "BEAM Transfer ID",
    prompt: "Enter a BEAM transfer ID.",
    ignoreFocusOut: true,
    validateInput: (value) => (value.trim() ? undefined : "Transfer ID is required.")
  });
}

async function confirm(message: string): Promise<boolean> {
  const selected = await vscode.window.showQuickPick(["No", "Yes"], {
    title: message,
    ignoreFocusOut: true
  });
  return selected === "Yes";
}

async function runBeamCommand(
  title: string,
  beam: BeamService,
  output: vscode.OutputChannel,
  action: () => Promise<void>
): Promise<void> {
  try {
    await vscode.window.withProgress(
      {
        location: vscode.ProgressLocation.Notification,
        title,
        cancellable: false
      },
      action
    );
  } catch (error) {
    const message = beam.describeError(error);
    output.appendLine(message);
    output.show(true);
    vscode.window.showErrorMessage(message);
  }
}

function validatePositiveNumber(value: string): string | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? undefined : "Value must be a positive number.";
}

function printStatus(output: vscode.OutputChannel, status: TransferStatusInfo): void {
  output.appendLine(`Transfer: ${status.transfer_id}`);
  output.appendLine(`Status:   ${status.status}`);
  output.appendLine(`Size:     ${status.source_bytes_total}`);
  output.appendLine(
    `Tasks:    ${status.delivery_tasks_completed}/${status.delivery_tasks_total}`
  );
  if (status.error_message) {
    output.appendLine(`Error:    ${status.error_message}`);
  }
}
