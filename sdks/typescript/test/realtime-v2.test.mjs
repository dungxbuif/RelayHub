import assert from "node:assert/strict";
import test from "node:test";
import {RelayHubRealtimeClient} from "../dist/esm/browser.js";

class FakeSocket {
  readyState = 1;
  protocol = "relayhub.realtime.v2";
  listeners = new Map();
  sent = [];
  addEventListener(type, listener) { const list = this.listeners.get(type) ?? []; list.push(listener); this.listeners.set(type, list); }
  send(data) { this.sent.push(JSON.parse(data)); }
  close() { this.readyState = 3; }
  emit(type, value) { for (const listener of this.listeners.get(type) ?? []) listener(value); }
}

test("realtime v2 negotiates, subscribes, targets, and updates presence", async () => {
  const socket = new FakeSocket();
  let protocols;
  const client = new RelayHubRealtimeClient({
    baseUrl: "https://relayhub.example",
    clientId: "client_1",
    channels: {room: ["subscribe", "publish", "presence"]},
    tokenProvider: async request => { assert.equal(request.clientId, "client_1"); return "short-token"; },
    socketFactory: (_url, selected) => { protocols = selected; queueMicrotask(() => socket.emit("message", {data: JSON.stringify({type: "ready", protocol: "relayhub.realtime.v2", client_id: "client_1"})})); return socket; },
  });
  await client.connect();
  assert.equal(protocols, "relayhub.realtime.v2");
  client.subscribe(["room"]);
  client.publish("room", {text: "hello"}, {type: "client", client_id: "client_2"});
  client.updatePresence("room", {status: "online"});
  client.unsubscribe(["room"]);
  assert.deepEqual(socket.sent.map(frame => frame.type), ["subscribe", "channel.publish", "presence.update", "unsubscribe"]);
});
