import type { ReactNode } from "react";
export function AsyncState({ title, children, role = "status" }: { title: string; children: ReactNode; role?: "status" | "alert" }) {
  return <section className="async-state" role={role}><h2>{title}</h2><div>{children}</div></section>;
}
