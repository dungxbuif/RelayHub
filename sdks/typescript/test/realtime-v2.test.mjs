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

test("realtime v2 supports bounded namespace grants, rewind, history, and batch outcomes", async () => {
  const socket = new FakeSocket();
  const histories = [];
  const batches = [];
  const client = new RelayHubRealtimeClient({
    baseUrl: "https://relayhub.example",
    clientId: "client_1",
    channels: {"tenant:42:*": ["subscribe", "publish", "history"]},
    tokenProvider: async () => "short-token",
    socketFactory: () => { queueMicrotask(() => socket.emit("message", {data: JSON.stringify({type: "ready", protocol: "relayhub.realtime.v2"})})); return socket; },
    onHistory: result => histories.push(result),
    onBatchResult: result => batches.push(result),
  });
  await client.connect();
  client.subscribe(["tenant:42:orders"], {limit: 10});
  client.history("tenant:42:orders", {limit: 20, cursor: "MTIzNC0w"});
  client.publishBatch([{id: "one", channel: "tenant:42:orders", data: {n: 1}}]);
  assert.deepEqual(socket.sent, [
    {type: "subscribe", channels: ["tenant:42:orders"], rewind: {limit: 10}},
    {type: "history.get", channel: "tenant:42:orders", limit: 20, cursor: "MTIzNC0w"},
    {type: "channel.publish.batch", items: [{id: "one", channel: "tenant:42:orders", data: {n: 1}}]},
  ]);
  socket.emit("message", {data: JSON.stringify({type: "history.result", channel: "tenant:42:orders", continuity_cursor: "MTIzNC0w", items: [{type: "channel.message", channel: "tenant:42:orders", data: {n: 1}, cursor: "MTIzNC0w"}]})});
  socket.emit("message", {data: JSON.stringify({type: "channel.publish.batch.result", outcomes: [{id: "one", accepted: true, message_id: "msg_1"}]})});
  assert.equal(histories.length, 1);
  assert.equal(histories[0].items[0].cursor, "MTIzNC0w");
  assert.equal(histories[0].continuityCursor, "MTIzNC0w");
  assert.deepEqual(batches[0].outcomes, [{id: "one", accepted: true, messageId: "msg_1", code: undefined}]);
});

test("realtime v2 rejects unbounded namespace grants and oversized batches", () => {
  const base = {baseUrl: "https://relayhub.example", clientId: "client_1", tokenProvider: async () => "token", socketFactory: () => new FakeSocket()};
  assert.throws(() => new RelayHubRealtimeClient({...base, channels: {"*": ["subscribe"]}}), /invalid realtime channel grant/);
  assert.throws(() => new RelayHubRealtimeClient({...base, channels: {room: ["admin"]}}), /invalid realtime actions/);
  const client = new RelayHubRealtimeClient({...base, channels: {room: ["publish"]}});
  assert.throws(() => client.publishBatch(Array.from({length: 51}, (_, index) => ({id: String(index), channel: "room", data: {}}))), /invalid realtime publish batch/);
  assert.throws(() => client.subscribe(Array.from({length: 11}, (_, index) => `room:${index}`), {limit: 10}), /invalid realtime rewind channels/);
});
