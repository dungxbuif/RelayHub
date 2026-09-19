import { lazy, Suspense } from "react";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { AuthProvider, useAuth } from "../auth/AuthProvider";
import { LoginPage } from "../auth/LoginPage";
import { AdminLayout } from "../layout/AdminLayout";
import { FeatureBoundaryPage } from "../pages/FeatureBoundaryPage";
import { NotFoundPage } from "../pages/NotFoundPage";

const OverviewPage = lazy(() => import("../pages/OverviewPage").then((module) => ({ default: module.OverviewPage })));
const EventsPage = lazy(() => import("../pages/EventsPage").then((module) => ({ default: module.EventsPage })));
const DeadLettersPage = lazy(() => import("../pages/DeadLettersPage").then((module) => ({ default: module.DeadLettersPage })));
const AuditLogsPage = lazy(() => import("../pages/AuditLogsPage").then((module) => ({ default: module.AuditLogsPage })));

const pages = [
  ["apps", "Apps", "Provision integrations and manage their delivery configuration."],
  ["routing-rules", "Routing Rules", "Control event routing with explicit, validated rules."],
  ["realtime-studio", "Realtime Studio", "Exercise realtime and durable WebSocket protocols safely."],
  ["system", "System", "Inspect dependency and replica health."],
] as const;

function ProtectedRoutes() {
  const { status } = useAuth();
  if (status !== "authenticated") return <LoginPage />;
  return <Suspense fallback={<main className="page" id="main-content" aria-busy="true">Loading Admin module…</main>}><Routes><Route element={<AdminLayout />}><Route index element={<OverviewPage />} />
    <Route path="events" element={<EventsPage />} />
    <Route path="dead-letters" element={<DeadLettersPage />} />
    <Route path="audit-logs" element={<AuditLogsPage />} />
    {pages.map(([path, title, description]) => <Route key={path} path={path} element={<FeatureBoundaryPage title={title} description={description} />} />)}
    <Route path="404" element={<NotFoundPage />} /><Route path="*" element={<Navigate to="404" replace />} />
  </Route></Routes></Suspense>;
}

export function App() {
  return <BrowserRouter basename="/admin"><AuthProvider><ProtectedRoutes /></AuthProvider></BrowserRouter>;
}
