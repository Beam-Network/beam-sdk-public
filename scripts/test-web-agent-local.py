"""Run the built SDK against the sibling agent's real, local WebSocket fixture.

Uses Go's source overlay; does not edit or run an installed agent, contact Beam
services, or read an existing identity. Requires cached Go modules and Chromium.
Wrap this process in OS-level localhost-only network isolation (see the docs).
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

sdk = Path(__file__).resolve().parent.parent
agent = sdk.parent / "beam-tunnel-agent"
original = agent / "apps/web-agent/internal/api/browser_test.go"
source = original.read_text()
anchor = 'filepath.Abs("../../browser-tests/example.mjs")'
if source.count(anchor) != 1:
    raise SystemExit("The sibling Go browser fixture changed; review the overlay adapter.")
node = os.environ.get("BEAM_WEB_BROWSER_NODE") or shutil.which("node")
if not node:
    raise SystemExit("Set BEAM_WEB_BROWSER_NODE to a local Node executable.")
with tempfile.TemporaryDirectory(prefix="beam-sdk-browser-") as temporary:
    temporary = Path(temporary)
    replacement = temporary / "browser_test.go"
    replacement.write_text(source.replace(anchor, 'filepath.Abs(os.Getenv("BEAM_SDK_BROWSER_SCRIPT"))'))
    overlay = temporary / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(original): str(replacement)}}))
    environment = {**os.environ, "BEAM_WEB_BROWSER_NODE": node, "BEAM_SDK_BROWSER_SCRIPT": str(sdk / "scripts/test-web-agent-browser.mjs"), "GOPROXY": "off", "GOSUMDB": "off", "GOFLAGS": "-buildvcs=false"}
    subprocess.run(["go", "test", "-overlay", str(overlay), "-race", "-count=1", "-run", "^TestBrowserExampleLocalFixture$", "-v", "-timeout", "100s", "./apps/web-agent/internal/api"], cwd=agent, env=environment, check=True)
