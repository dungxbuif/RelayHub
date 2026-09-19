import { describe, expect, it } from "vitest";

import { redactText } from "../src/api/client";

describe("redactText", () => {
  it("removes credentials and socket query tokens", () => {
    const input = "Authorization: Bearer bearer-secret X-RelayHub-Api-Key: rhk_secret X-RelayHub-Signature: deadbeef /ws?token=socket-secret";
    const output = redactText(input);
    for (const secret of ["bearer-secret", "rhk_secret", "deadbeef", "socket-secret"]) {
      expect(output).not.toContain(secret);
    }
    expect(output).toContain("[REDACTED]");
  });

  it("bounds untrusted error text", () => {
    expect(redactText("x".repeat(4_000))).toHaveLength(1_024);
  });
});
