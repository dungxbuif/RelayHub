import { lazy, Suspense } from "react";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { AuthProvider, useAuth } from "../auth/AuthProvider";
import { LoginPage } from "../auth/LoginPage";
import { AdminLayout } from "../layout/AdminLayout";
import { NotFoundPage } from "../pages/NotFoundPage";

const OverviewPage = lazy(() => import("../pages/OverviewPage").then((module) => ({ default: module.OverviewPage })));
const EventsPage = lazy(() => import("../pages/EventsPage").then((module) => ({ default: module.EventsPage })));
const DeadLettersPage = lazy(() => import("../pages/DeadLettersPage").then((module) => ({ default: module.DeadLettersPage })));
const AuditLogsPage = lazy(() => import("../pages/AuditLogsPage").then((module) => ({ default: module.AuditLogsPage })));
const EventDetailPage = lazy(() => import("../pages/EventDetailPage").then((module) => ({ default: module.EventDetailPage })));
const DeadLetterDetailPage = lazy(() => import("../pages/DeadLetterDetailPage").then((module) => ({ default: module.DeadLetterDetailPage })));
const AppsPage = lazy(() => import("../pages/AppsPage").then((module) => ({ default: module.AppsPage })));
const RoutingRulesPage = lazy(() => import("../pages/RoutingRulesPage").then((module) => ({ default: module.RoutingRulesPage })));
const RealtimeStudioPage = lazy(() => import("../pages/RealtimeStudioPage").then((module) => ({ default: module.RealtimeStudioPage })));
const QueuePage = lazy(() => import("../pages/QueuePage").then((module) => ({ default: module.QueuePage })));
const SystemPage = lazy(() => import("../pages/SystemPage").then((module) => ({ default: module.SystemPage })));
const AdminUsersPage = lazy(() => import("../pages/AdminUsersPage").then((module) => ({ default: module.AdminUsersPage })));
const FunctionsPage = lazy(() => import("../pages/FunctionsPage").then((module) => ({ default: module.FunctionsPage })));

function ProtectedRoutes() {
  const { status } = useAuth();
  if (status !== "authenticated") return <LoginPage />;
  return <Suspense fallback={<main className="page" id="main-content" aria-busy="true">Loading Admin module…</main>}><Routes><Route element={<AdminLayout />}><Route index element={<OverviewPage />} />
    <Route path="dashboard/*" element={<Navigate to="/" replace />} />
    <Route path="events" element={<EventsPage />} />
    <Route path="events/:eventID" element={<EventDetailPage />} />
    <Route path="dead-letters" element={<DeadLettersPage />} />
    <Route path="dead-letters/:deliveryID" element={<DeadLetterDetailPage />} />
    <Route path="audit-logs" element={<AuditLogsPage />} />
    <Route path="apps" element={<AppsPage />} />
    <Route path="functions" element={<FunctionsPage />} />
    <Route path="users" element={<AdminUsersPage />} />
    <Route path="routing-rules" element={<RoutingRulesPage />} />
    <Route path="realtime-studio" element={<RealtimeStudioPage />} />
    <Route path="queue" element={<QueuePage />} />
    <Route path="system" element={<SystemPage />} />
    <Route path="404" element={<NotFoundPage />} /><Route path="*" element={<Navigate to="/404" replace />} />
  </Route></Routes></Suspense>;
}

export function App() {
  return <BrowserRouter basename="/admin"><AuthProvider><ProtectedRoutes /></AuthProvider></BrowserRouter>;
}
