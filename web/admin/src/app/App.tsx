import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { AuthProvider, useAuth } from "../auth/AuthProvider";
import { LoginPage } from "../auth/LoginPage";
import { AdminLayout } from "../layout/AdminLayout";
import { FeatureBoundaryPage } from "../pages/FeatureBoundaryPage";
import { NotFoundPage } from "../pages/NotFoundPage";
import { OverviewPage } from "../pages/OverviewPage";

const pages = [
  ["events", "Events", "Search and inspect durable events across applications."],
  ["dead-letters", "Dead Letters", "Inspect failed deliveries and perform generation-fenced replay."],
  ["apps", "Apps", "Provision integrations and manage their delivery configuration."],
  ["routing-rules", "Routing Rules", "Control event routing with explicit, validated rules."],
  ["realtime-studio", "Realtime Studio", "Exercise realtime and durable WebSocket protocols safely."],
  ["audit-logs", "Audit Logs", "Review append-only operator and system actions."],
  ["system", "System", "Inspect dependency and replica health."],
] as const;

function ProtectedRoutes() {
  const { status } = useAuth();
  if (status !== "authenticated") return <LoginPage />;
  return <Routes><Route element={<AdminLayout />}><Route index element={<OverviewPage />} />
    {pages.map(([path, title, description]) => <Route key={path} path={path} element={<FeatureBoundaryPage title={title} description={description} />} />)}
    <Route path="404" element={<NotFoundPage />} /><Route path="*" element={<Navigate to="404" replace />} />
  </Route></Routes>;
}

export function App() {
  return <BrowserRouter basename="/admin"><AuthProvider><ProtectedRoutes /></AuthProvider></BrowserRouter>;
}
