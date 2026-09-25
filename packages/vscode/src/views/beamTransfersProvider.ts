import * as vscode from "vscode";
import type { StoredTransfer, TransferStore } from "../services/transferStore.js";

export class BeamTransfersProvider implements vscode.TreeDataProvider<TransferTreeItem> {
  private readonly onDidChangeTreeDataEmitter = new vscode.EventEmitter<TransferTreeItem | undefined>();
  readonly onDidChangeTreeData = this.onDidChangeTreeDataEmitter.event;

  constructor(private readonly store: TransferStore) {}

  refresh(): void {
    this.onDidChangeTreeDataEmitter.fire(undefined);
  }

  getTreeItem(element: TransferTreeItem): vscode.TreeItem {
    return element;
  }

  getChildren(): TransferTreeItem[] {
    return this.store.getAll().map((transfer) => new TransferTreeItem(transfer));
  }
}

export class TransferTreeItem extends vscode.TreeItem {
  readonly contextValue = "beamTransfer";

  constructor(readonly transfer: StoredTransfer) {
    super(transfer.name || transfer.transferId, vscode.TreeItemCollapsibleState.None);
    this.description = transfer.status || "created";
    this.tooltip = tooltipFor(transfer);
    this.command = {
      command: "beam.checkStatus",
      title: "Check Transfer Status",
      arguments: [this]
    };
    this.iconPath = new vscode.ThemeIcon(iconForStatus(transfer.status));
  }
}

function tooltipFor(transfer: StoredTransfer): string {
  const lines = [`Transfer: ${transfer.transferId}`];
  if (transfer.name) {
    lines.push(`Name: ${transfer.name}`);
  }
  if (transfer.status) {
    lines.push(`Status: ${transfer.status}`);
  }
  if (transfer.totalChunks !== undefined) {
    lines.push(`Chunks: ${transfer.chunksCompleted ?? 0}/${transfer.totalChunks}`);
  }
  lines.push(`Updated: ${transfer.updatedAt}`);
  return lines.join("\n");
}

function iconForStatus(status: string | undefined): string {
  switch (status?.toLowerCase()) {
    case "completed":
      return "pass-filled";
    case "failed":
      return "error";
    case "cancelled":
    case "canceled":
      return "circle-slash";
    case "running":
    case "distributing":
    case "in_progress":
      return "sync";
    default:
      return "cloud-upload";
  }
}
