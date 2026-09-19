import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

export function usePausableRefresh<T>(key: readonly unknown[], load: (signal: AbortSignal) => Promise<T>) {
  const [paused, setPaused] = useState(false);
  const query = useQuery({ queryKey: key, queryFn: ({ signal }) => load(signal), refetchInterval: paused ? false : 5_000, refetchIntervalInBackground: false });
  return { ...query, paused, togglePaused: () => setPaused((value) => !value) };
}
