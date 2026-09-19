import { describe, expect, it } from "vitest";
import { batchPublishFrame, historyFrame, rewindSubscribeFrame } from "../src/pages/realtimeStudioFrames";

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
});
