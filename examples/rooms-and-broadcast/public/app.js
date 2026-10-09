/**
 * Example application.
 *
 * The point of this file is how short it is: no Beam credentials, no NATS, no
 * relays, no orchestrators. A token endpoint and four calls.
 */

// Served from packages/web-sdk/dist by this example's server. In a real app
// this is `import { Beam } from "@beam-network/web-sdk"`.
import { Beam } from "/vendor/index.js";

const beam = new Beam({
  // The only configuration required. The broker decides who this visitor is and
  // what they may do; the browser just asks it for a token.
  tokenEndpoint: "/api/beam/token",
  controlUrl: "/api/beam",
  debug: true,
});

const el = (id) => document.getElementById(id);

function log(listId, message) {
  const item = document.createElement("li");
  item.textContent = `${new Date().toLocaleTimeString()}  ${message}`;
  const list = el(listId);
  list.prepend(item);
  while (list.children.length > 40) list.lastChild.remove();
}

// ── session ────────────────────────────────────────────────────────────────

// Optional: mints the first token up front so a misconfigured broker shows up
// now rather than when someone clicks a button.
beam
  .connect()
  .then((token) => {
    el("session-state").textContent = "connected";
    el("session-state").classList.add("ok");
    el("session-scopes").textContent = token.scopes.join(" · ");
  })
  .catch((error) => {
    el("session-state").textContent = "token failed";
    el("session-state").classList.add("bad");
    log("room-events", `could not obtain a token: ${error.message}`);
  });

// ── rooms ──────────────────────────────────────────────────────────────────

let room;
let session;

function addVideo(stream, { muted = false, label = "" } = {}) {
  const wrapper = document.createElement("figure");
  const video = document.createElement("video");
  video.autoplay = true;
  video.playsInline = true;
  video.muted = muted;
  video.srcObject = stream;
  wrapper.append(video);
  if (label) {
    const caption = document.createElement("figcaption");
    caption.textContent = label;
    wrapper.append(caption);
  }
  el("videos").append(wrapper);
  return wrapper;
}

el("create-room").addEventListener("click", async () => {
  try {
    room = await beam.rooms.create({ name: "example" });
    el("room-id").textContent = `room ${room.id}`;
    el("join-camera").disabled = false;
    el("join-viewer").disabled = false;
    log("room-events", `created room ${room.id}`);
  } catch (error) {
    log("room-events", `create failed — ${error.code}: ${error.message}`);
  }
});

async function join({ publish }) {
  try {
    const stream = publish ? await beam.media.camera() : undefined;
    session = await beam.rooms.join(room, { displayName: "example", ...(stream ? { publish: stream } : {}) });

    if (stream) addVideo(stream, { muted: true, label: "you" });

    session.on("state", (state) => log("room-events", `session ${state}`));
    session.on("participant.joined", (participant) =>
      log("room-events", `${participant.displayName ?? participant.id} joined`),
    );
    session.on("participant.left", ({ id }) => log("room-events", `${id} left`));
    session.on("track.added", ({ stream: remote, participantId }) => {
      log("room-events", `track from ${participantId ?? "unknown"}`);
      addVideo(remote, { label: participantId ?? "remote" });
    });
    session.on("error", (error) => log("room-events", `error: ${error.message}`));

    el("leave").disabled = false;
    el("join-camera").disabled = true;
    el("join-viewer").disabled = true;
  } catch (error) {
    log("room-events", `join failed — ${error.code}: ${error.message}`);
  }
}

el("join-camera").addEventListener("click", () => join({ publish: true }));
el("join-viewer").addEventListener("click", () => join({ publish: false }));

el("leave").addEventListener("click", async () => {
  await session?.leave();
  session = undefined;
  el("videos").replaceChildren();
  el("leave").disabled = true;
  el("join-camera").disabled = false;
  el("join-viewer").disabled = false;
  log("room-events", "left the room");
});

// ── transfers ──────────────────────────────────────────────────────────────

let transfer;

function formatBytes(bytes) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
}

el("start-transfer").addEventListener("click", async () => {
  try {
    // The browser names endpoints. It never sees a bucket, a region, or a key —
    // the broker resolves these references against configuration it holds.
    transfer = await beam.transfers.create({
      source: el("source").value,
      destination: el("destination").value,
    });

    el("cancel-transfer").disabled = false;
    log("transfer-events", `started ${transfer.id}`);

    transfer.on("progress", (progress) => {
      const percent = progress.percent ?? 0;
      el("progress-bar").style.width = `${percent}%`;
      el("transfer-state").textContent =
        `${progress.status} — ${formatBytes(progress.bytesCompleted)}` +
        (progress.totalBytes ? ` of ${formatBytes(progress.totalBytes)} (${percent.toFixed(0)}%)` : "");
    });
    transfer.on("status", (status) => log("transfer-events", `status ${status}`));

    const result = await transfer.done();
    log("transfer-events", `completed ${result.id}`);
  } catch (error) {
    log("transfer-events", `${error.code ?? "error"}: ${error.message}`);
  } finally {
    el("cancel-transfer").disabled = true;
  }
});

el("cancel-transfer").addEventListener("click", async () => {
  await transfer?.cancel();
  log("transfer-events", "cancel requested");
});
