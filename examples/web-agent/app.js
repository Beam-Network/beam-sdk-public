import { WebAgent, media } from "/sdk/index.js";
const element = (id) => document.getElementById(id);
let agent, subscription, publisher, viewer;
const scope = () => ({ roomId: element("room").value, channelId: element("channel").value });
const log = (value) => {
  element("log").textContent =
    `${typeof value === "string" ? value : JSON.stringify(value)}\n${element("log").textContent}`.slice(0, 12000);
};
const action = (id, run) =>
  element(id).addEventListener("click", () =>
    Promise.resolve()
      .then(run)
      .catch((error) => log(error.message)),
  );
function broadcasts(items) {
  const select = element("broadcast"),
    previous = select.value;
  select.replaceChildren(...items.map((item) => new window.Option(item.id, item.id)));
  if (items.some((item) => item.id === previous)) select.value = previous;
}
action("connect", async () => {
  agent?.disconnect();
  const credential = element("credential").value;
  element("credential").value = "";
  agent = new WebAgent({ url: element("url").value, getCredential: () => credential });
  agent.on("state", ({ state }) => {
    element("state").textContent = state;
    if (state === "disconnected") {
      subscription = publisher = viewer = undefined;
      element("local").srcObject = element("remote").srcObject = null;
      broadcasts([]);
    }
  });
  agent.on("error", (error) => log(error.message));
  agent.on("membership", (membership) => log({ membership }));
  await agent.connect();
  log("Connected. Subscriptions and publications are not automatically replayed.");
});
action("disconnect", () => {
  agent?.disconnect();
  agent = undefined;
  subscription = publisher = viewer = undefined;
});
action("join", async () => {
  const invitation = element("invitation").value;
  element("invitation").value = "";
  await agent.joinRoom(scope().roomId, { invitation });
  await discover();
});
async function discover() {
  const channels = await agent.listChannels(scope().roomId);
  element("channel").replaceChildren(
    ...channels
      .filter((channel) => channel.kind !== "stream")
      .map((channel) => {
        const option = new window.Option(`${channel.name} (${channel.kind})`, channel.id);
        option.dataset.kind = channel.kind;
        return option;
      }),
  );
}
action("discover", discover);
action("leave", () => agent.leaveRoom(scope().roomId));
action("subscribe", async () => {
  await subscription?.close();
  const kind = element("channel").selectedOptions[0]?.dataset.kind;
  subscription = await agent.subscribe(scope(), kind);
  subscription.on("message", (value) => log(new window.TextDecoder().decode(value.payload)));
  subscription.on("broadcasts", broadcasts);
  subscription.on("error", (error) => log(error.message));
  await subscription.ready;
  broadcasts(subscription.broadcasts);
  log("Subscription ready.");
});
action("unsubscribe", async () => {
  await subscription?.close();
  subscription = undefined;
});
action("send", async () => log(await agent.publishMessage(scope(), element("message").value)));
async function publish(capture) {
  await publisher?.close();
  const stream = await capture();
  try {
    publisher = await agent.publishMedia(scope(), stream, { stopTracksOnClose: true });
    element("local").srcObject = stream;
    publisher.on("error", (error) => log(error.message));
    await publisher.ready;
    log("Publishing media.");
  } catch (error) {
    media.stopStream(stream);
    throw error;
  }
}
action("camera", () => publish(() => media.camera()));
action("screen", () => publish(() => media.screen()));
action("stopPublish", async () => {
  await publisher?.close();
  publisher = undefined;
});
action("watch", async () => {
  await viewer?.close();
  viewer = await agent.watchMedia(subscription, element("broadcast").value || undefined);
  element("remote").srcObject = viewer.stream;
  viewer.on("error", (error) => log(error.message));
  await viewer.ready;
  await element("remote").play();
  log("Viewing media.");
});
action("stopWatch", async () => {
  await viewer?.close();
  viewer = undefined;
});
window.addEventListener("pagehide", () => agent?.disconnect());
