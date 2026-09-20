import { describe, expect, it } from "vitest";
import { batchPublishFrame, filePublishFrame, historyFrame, listActionsFrame, putActionFrame, removeActionFrame, rewindSubscribeFrame } from "../src/pages/realtimeStudioFrames";

describe("Realtime Studio advanced frames", () => {
  it("builds bounded history and rewind frames", () => {
    expect(historyFrame("project:42:orders", 50)).toEqual({type: "history.get", channel: "project:42:orders", limit: 50});
    expect(rewindSubscribeFrame("project:42:orders", 25)).toEqual({type: "subscribe", channels: ["project:42:orders"], rewind: {limit: 25}});
    expect(() => historyFrame("project:42:orders", 101)).toThrow(/history limit/);
  });

  it("builds one identifiable batch item", () => {
    expect(batchPublishFrame("item-1", "project:42:orders", {n: 1}, {type: "all"})).toEqual({
      type: "channel.publish.batch",
      items: [{id: "item-1", channel: "project:42:orders", audience: {type: "all"}, data: {n: 1}}],
    });
  });

  it("builds message action frames without client-supplied actor identity", () => {
    expect(putActionFrame("room", "msg_1", "reaction", "idem_1", {emoji: "ok"})).toEqual({type: "message.action.put", channel: "room", message_id: "msg_1", action_type: "reaction", idempotency_key: "idem_1", data: {emoji: "ok"}});
    expect(listActionsFrame("room", "msg_1")).toEqual({type: "message.actions.get", channel: "room", message_id: "msg_1"});
    expect(removeActionFrame("room", "msg_1", "action_1")).toEqual({type: "message.action.remove", channel: "room", message_id: "msg_1", action_id: "action_1"});
    expect(filePublishFrame("room", "file_1")).toEqual({type: "file.publish", channel: "room", file_id: "file_1", audience: {type: "all"}});
  });
});
