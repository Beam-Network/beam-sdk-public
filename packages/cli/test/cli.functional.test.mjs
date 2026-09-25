import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

const cliPath = fileURLToPath(new URL("../dist/cli.js", import.meta.url));

test("beam-send rejects invalid JSON input before opening lifecycle transport", async () => {
  const result = await runCli([
    "create",
    "--api-key",
    "b1m_cli",
    "--source",
    "{nope",
    "--destination",
    '{"type":"http","url":"https://dest.example/file.bin"}',
    "--total-size",
    "1024"
  ]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /Invalid JSON for --source/);
});

test("beam-send rejects HTTP lifecycle endpoints", async () => {
  const result = await runCli([
    "status",
    "transfer_cli",
    "--api-key",
    "b1m_cli",
    "--server",
    "http://beamcore.test",
    "--json"
  ]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /must be nats:\/\/, tls:\/\//);
});

function runCli(args) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [cliPath, ...args], {
      env: { ...process.env, BEAM_API_KEY: "", BEAM_NATS_URL: "" },
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (data) => {
      stdout += data;
    });
    child.stderr.on("data", (data) => {
      stderr += data;
    });
    child.on("close", (code) => {
      resolve({ code, stdout, stderr });
    });
  });
}