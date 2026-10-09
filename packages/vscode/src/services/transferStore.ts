import type * as vscode from "vscode";
import type { TransferStatusInfo } from "@beam-network/sdk";

const storageKey = "beam.recentTransfers";
const maxTransfers = 25;

export interface StoredTransfer {
  transferId: string;
  name?: string;
  status?: string;
  totalChunks?: number;
  chunksCompleted?: number;
  updatedAt: string;
  rawStatus?: TransferStatusInfo;
}

export class TransferStore {
  constructor(private readonly state: vscode.Memento) {}

  getAll(): StoredTransfer[] {
    return this.state.get<StoredTransfer[]>(storageKey, []);
  }

  async upsert(transfer: Omit<StoredTransfer, "updatedAt">): Promise<void> {
    const current = this.getAll().filter((item) => item.transferId !== transfer.transferId);
    const next: StoredTransfer[] = [
      {
        ...transfer,
        updatedAt: new Date().toISOString()
      },
      ...current
    ].slice(0, maxTransfers);

    await this.state.update(storageKey, next);
  }

  async updateStatus(status: TransferStatusInfo): Promise<void> {
    await this.upsert({
      transferId: status.transfer_id,
      name: status.name,
      status: status.status,
      totalChunks: status.delivery_tasks_total,
      chunksCompleted: status.delivery_tasks_completed,
      rawStatus: status
    });
  }
}
