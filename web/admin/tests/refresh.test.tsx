import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { usePausableRefresh } from "../src/hooks/usePausableRefresh";

describe("pausable dashboard refresh", () => {
  afterEach(() => vi.useRealTimers());

  it("does not overlap a refresh with an unfinished request", async () => {
    vi.useFakeTimers();
    let resolveFirst!: (value: string) => void;
    const first = new Promise<string>((resolve) => { resolveFirst = resolve; });
    const load = vi.fn().mockImplementationOnce(() => first).mockResolvedValue("next");
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: PropsWithChildren) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    renderHook(() => usePausableRefresh(["probe"], load), { wrapper });
    await act(async () => { await Promise.resolve(); });
    expect(load).toHaveBeenCalledTimes(1);
    await act(async () => { vi.advanceTimersByTime(15_000); await Promise.resolve(); });
    expect(load).toHaveBeenCalledTimes(1);
    await act(async () => { resolveFirst("first"); await Promise.resolve(); });
    await act(async () => { vi.advanceTimersByTime(5_000); await Promise.resolve(); });
    expect(load).toHaveBeenCalledTimes(2);
    client.clear();
  });
});
