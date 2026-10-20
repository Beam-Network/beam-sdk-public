#!/usr/bin/env python3
"""Example compute agent: a Room member that answers jobs a contract requests (`requestTransfer` kind 3).

1. Follows `TransferRequested` logs of kind 3 on the receipts contract. The log carries the request id and the
   job's instruction (`input`).
2. Waits for the job's input: the next object Beam delivers to this agent's Room inbox. The Studio input workflow
   publishes it under `request:<requestId>`, but a target never sees that key, so jobs are served one at a time,
   in request order.
3. Computes per-sensor statistics over the input CSV and writes them to the fixed output file.
4. Starts the Studio output workflow through its signed webhook with `{"requestId": ...}`. That workflow publishes
   the output file from this agent under the same key, which is where the receipts workflow looks it up.

Standard library only. Configuration: agent.json beside this file (see agent.example.json). The webhook URL and
signing secret are read from files only this account can read.
"""

import hashlib
import hmac
import json
import os
import statistics
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG = json.load(open(os.path.join(HERE, "agent.json")))
STATE_PATH = os.path.join(HERE, "state.json")
EXPECTED_HEADER = "timestamp,sensor,temperature_c,humidity_pct"


def log(message):
    print(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), message, flush=True)


def rpc(method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    # Public RPC gateways refuse urllib's default User-Agent.
    request = urllib.request.Request(CONFIG["rpcUrl"], data=body, headers={
        "content-type": "application/json", "user-agent": "beam-pilot-compute-agent/1"})
    with urllib.request.urlopen(request, timeout=30) as response:
        answer = json.load(response)
    if "error" in answer:
        raise RuntimeError(f"{method}: {answer['error']}")
    return answer["result"]


def load_state():
    try:
        return json.load(open(STATE_PATH))
    except FileNotFoundError:
        return {"nextBlock": CONFIG["startBlock"], "queue": [], "done": {}, "consumedObjects": []}


def save_state(state):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=1)
    os.replace(tmp, STATE_PATH)


def decode_job(entry):
    """`TransferRequested(bytes32 indexed requestId, address indexed requester, uint8 kind, string input)`."""
    data = bytes.fromhex(entry["data"][2:])
    kind = int.from_bytes(data[0:32], "big")
    offset = int.from_bytes(data[32:64], "big")
    length = int.from_bytes(data[offset:offset + 32], "big")
    instruction = data[offset + 32:offset + 32 + length].decode("utf-8", "replace")
    return {
        "requestId": entry["topics"][1].lower(),
        "requester": "0x" + entry["topics"][2][-40:].lower(),
        "kind": kind,
        "instruction": instruction,
        "block": int(entry["blockNumber"], 16),
        "tx": entry["transactionHash"],
    }


def follow_requests(state):
    head = int(rpc("eth_blockNumber", []), 16) - CONFIG["confirmations"]
    while state["nextBlock"] <= head:
        to_block = min(head, state["nextBlock"] + CONFIG["logRangeBlocks"] - 1)
        entries = rpc("eth_getLogs", [{
            "address": CONFIG["receiptsAddress"],
            "topics": [CONFIG["transferRequestedTopic"]],
            "fromBlock": hex(state["nextBlock"]),
            "toBlock": hex(to_block),
        }])
        for entry in sorted(entries, key=lambda e: (int(e["blockNumber"], 16), int(e["logIndex"], 16))):
            job = decode_job(entry)
            if job["kind"] != 3:
                continue
            if job["requestId"] in state["done"] or any(q["requestId"] == job["requestId"] for q in state["queue"]):
                continue
            log(f"job {job['requestId']} requested in {job['tx']}: {job['instruction']!r}")
            state["queue"].append(job)
        state["nextBlock"] = to_block + 1
        save_state(state)


def inbox_objects():
    room_dir = os.path.join(
        os.path.expanduser(CONFIG["roomInbox"]), "objects", hashlib.sha256(CONFIG["roomId"].encode()).hexdigest()
    )
    if not os.path.isdir(room_dir):
        return []
    names = [n for n in os.listdir(room_dir) if n.endswith(".object")]
    return sorted((os.path.getmtime(os.path.join(room_dir, n)), os.path.join(room_dir, n)) for n in names)


def compute(path, job):
    raw = open(path, "rb").read()
    lines = raw.decode("utf-8").strip().splitlines()
    header = lines[0].split(",")
    columns = {name: i for i, name in enumerate(header)}
    readings = {}
    for line in lines[1:]:
        cells = line.split(",")
        sensor = cells[columns["sensor"]]
        readings.setdefault(sensor, {"temperature_c": [], "humidity_pct": []})
        readings[sensor]["temperature_c"].append(float(cells[columns["temperature_c"]]))
        readings[sensor]["humidity_pct"].append(float(cells[columns["humidity_pct"]]))
    summary = {
        sensor: {
            metric: {
                "count": len(values),
                "mean": round(statistics.fmean(values), 3),
                "min": min(values),
                "max": max(values),
                "stdev": round(statistics.stdev(values), 3),
            }
            for metric, values in metrics.items()
        }
        for sensor, metrics in sorted(readings.items())
    }
    anomalies = sorted(
        sensor for sensor, metrics in summary.items() if metrics["temperature_c"]["max"] > CONFIG["alertTemperatureC"]
    )
    return {
        "type": "beam.pilot.job_result.v1",
        "requestId": job["requestId"],
        "instruction": job["instruction"],
        "input": {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw), "rows": len(lines) - 1},
        "result": {"sensors": summary, "sensorsAboveAlert": anomalies, "alertTemperatureC": CONFIG["alertTemperatureC"]},
        "computedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def write_output(result):
    path = CONFIG["outputPath"]
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(result, f, indent=1, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def start_output_workflow(request_id):
    url = open(os.path.expanduser(CONFIG["outputWebhookUrlFile"])).read().strip()
    secret = open(os.path.expanduser(CONFIG["outputWebhookSecretFile"])).read().strip()
    body = json.dumps({"requestId": request_id}, separators=(",", ":"))
    timestamp = str(int(time.time()))
    signature = hmac.new(secret.encode(), f"v1:{timestamp}:{body}".encode(), hashlib.sha256).hexdigest()
    request = urllib.request.Request(url, data=body.encode(), method="POST", headers={
        "content-type": "application/json",
        "idempotency-key": request_id,
        "x-beam-timestamp": timestamp,
        "x-beam-signature": f"v1={signature}",
    })
    with urllib.request.urlopen(request, timeout=30) as response:
        answer = json.loads(response.read() or b"{}")
        return response.status, answer.get("runId") or answer.get("run", {}).get("id")


def serve(state):
    if not state["queue"]:
        return
    job = state["queue"][0]
    fresh = [path for _, path in inbox_objects() if path not in state["consumedObjects"]]
    if not fresh:
        return
    path = fresh[0]
    with open(path, "rb") as f:
        header = f.readline().decode("utf-8", "replace").strip()
    if header != EXPECTED_HEADER:
        # Another delivery to this Room's inbox, such as a file on a different channel.
        log(f"skipping inbox object {os.path.basename(path)}: not a job input")
        state["consumedObjects"].append(path)
        save_state(state)
        return
    result = compute(path, job)
    output_sha256 = write_output(result)
    log(f"job {job['requestId']}: input {result['input']['sha256'][:16]}… ({result['input']['rows']} rows), "
        f"output {output_sha256[:16]}…, sensors above {CONFIG['alertTemperatureC']} °C: {result['result']['sensorsAboveAlert']}")
    status, run_id = start_output_workflow(job["requestId"])
    log(f"job {job['requestId']}: output workflow answered HTTP {status}, run {run_id}")
    state["consumedObjects"].append(path)
    state["done"][job["requestId"]] = {"input": path, "inputSha256": result["input"]["sha256"],
                                       "outputSha256": output_sha256, "run": run_id}
    state["queue"].pop(0)
    save_state(state)
    # The output workflow reads the fixed output file; a newer result must not replace it mid-send.
    time.sleep(CONFIG["cooldownSeconds"])


def main():
    state = load_state()
    if not os.path.exists(STATE_PATH):
        # Objects already in the inbox belong to earlier deliveries, never to a job requested from now on.
        state["consumedObjects"] = [path for _, path in inbox_objects()]
        save_state(state)
    log(f"following {CONFIG['receiptsAddress']} from block {state['nextBlock']}; {len(state['queue'])} jobs queued")
    while True:
        try:
            follow_requests(state)
            serve(state)
        except Exception as error:  # a flaky RPC or Studio answer is retried on the next pass
            log(f"pass failed: {type(error).__name__}: {error}")
        time.sleep(CONFIG["pollSeconds"])
        if "--once" in sys.argv:
            break


if __name__ == "__main__":
    main()
