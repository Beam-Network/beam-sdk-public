import type * as vscode from "vscode";

const apiKeySecret = "beam.apiKey";

export class CredentialService {
  constructor(private readonly secrets: vscode.SecretStorage) {}

  getApiKey(): Thenable<string | undefined> {
    return this.secrets.get(apiKeySecret);
  }

  setApiKey(apiKey: string): Thenable<void> {
    return this.secrets.store(apiKeySecret, apiKey);
  }

  clearApiKey(): Thenable<void> {
    return this.secrets.delete(apiKeySecret);
  }
}
