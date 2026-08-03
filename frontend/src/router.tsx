import {
  createRootRoute,
  createRoute,
  createRouter,
  Link,
  Outlet,
} from "@tanstack/react-router";
import { useEventStream } from "./api/useEventStream";
import { clearToken, getToken } from "./api/auth";
import { BoardView } from "./views/BoardView";
import { PipelineView } from "./views/PipelineView";
import { DlqView } from "./views/DlqView";
import { WaitingView } from "./views/WaitingView";
import { JobDetailView } from "./views/JobDetailView";
import { FleetView } from "./views/FleetView";
import { WorkerDetailView } from "./views/WorkerDetailView";
import { QueuesView } from "./views/QueuesView";
import { CronView } from "./views/CronView";

const NAV: { to: string; label: string }[] = [
  { to: "/", label: "Board" },
  { to: "/pipeline", label: "Pipeline" },
  { to: "/dlq", label: "Failed / DLQ" },
  { to: "/waiting", label: "Waiting" },
  { to: "/fleet", label: "Fleet" },
  { to: "/queues", label: "Queues" },
  { to: "/cron", label: "Cron" },
];

function Shell() {
  useEventStream(); // one SSE subscription for the whole app (live updates)
  return (
    <div className="min-h-screen">
      <header className="border-b border-symba-border bg-symba-panel px-6 py-3">
        <div className="flex items-center gap-6">
          <span className="text-lg font-bold text-symba-text">Symba</span>
          <nav className="flex gap-1">
            {NAV.map((n) => (
              <Link
                key={n.to}
                to={n.to}
                className="rounded px-3 py-1.5 text-sm text-symba-muted hover:bg-symba-border hover:text-symba-text [&.active]:bg-indigo-600 [&.active]:text-white"
                activeOptions={{ exact: n.to === "/" }}
              >
                {n.label}
              </Link>
            ))}
          </nav>
          {/* Sign out only shows when a token is stored (token-mode); mode=none has
              no credential, so there's nothing to clear. */}
          {getToken() !== null && (
            <button
              type="button"
              onClick={() => clearToken()}
              className="ml-auto rounded px-3 py-1.5 text-sm text-symba-muted hover:bg-symba-border hover:text-symba-text"
            >
              Sign out
            </button>
          )}
        </div>
      </header>
      <main className="mx-auto max-w-7xl p-6">
        <Outlet />
      </main>
    </div>
  );
}

const rootRoute = createRootRoute({ component: Shell });

const route = (path: string, component: () => React.JSX.Element) =>
  createRoute({ getParentRoute: () => rootRoute, path, component });

const routeTree = rootRoute.addChildren([
  createRoute({ getParentRoute: () => rootRoute, path: "/", component: BoardView }),
  route("/pipeline", PipelineView),
  route("/dlq", DlqView),
  route("/waiting", WaitingView),
  route("/fleet", FleetView),
  route("/queues", QueuesView),
  route("/cron", CronView),
  createRoute({
    getParentRoute: () => rootRoute,
    path: "/fleet/$workerId",
    component: WorkerDetailView,
  }),
  createRoute({
    getParentRoute: () => rootRoute,
    path: "/jobs/$jobId",
    component: JobDetailView,
  }),
]);

export const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
