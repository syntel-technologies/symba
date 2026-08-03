import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import { router } from "./router";
import { LoginGate } from "./components/LoginGate";
import "./index.css";

// SSE drives freshness (useEventStream invalidates on job_events), so polling is a
// slow safety net only — not the primary refresh path.
const queryClient = new QueryClient({
  defaultOptions: { queries: { staleTime: 5_000, refetchInterval: 15_000 } },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <LoginGate>
        <RouterProvider router={router} />
      </LoginGate>
    </QueryClientProvider>
  </StrictMode>,
);
