import type { ReactNode } from "react";
import type { AdminUserPrincipal } from "../api/types";
import { useAdminPrincipal } from "./AuthProvider";

export function canMutate(principal: AdminUserPrincipal | null): boolean {
  return principal?.role === "admin";
}

export function PermissionGate({ children, mutation = false, fallback = <span role="status">Read-only access</span> }: { children: ReactNode; mutation?: boolean; fallback?: ReactNode }) {
  const principal = useAdminPrincipal();
  if (mutation && !canMutate(principal)) return <>{fallback}</>;
  return <>{children}</>;
}
