import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { Background, Controls, ReactFlow, type Edge, type Node } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { api } from "../api/client";
import { ErrorBox, Loading, Panel } from "../components/Panel";
import { stateColor } from "../components/StateBadge";

// Pipeline per ctx_id: React Flow DAG. Nodes = jobs in the context (color
// = state, WAITING nodes annotated with wait_key). Edges = dep/chain lineage from
// /tree. Layout is a simple longest-path layering — cheap, deterministic, no external
// layout engine (avoids a heavyweight dagre dep for what is usually a small DAG).
function layout(
  jobIds: string[],
  edges: { upstream: string; downstream: string }[],
): Map<string, { x: number; y: number }> {
  const depth = new Map<string, number>();
  for (const id of jobIds) depth.set(id, 0);
  // Relax depths over |V| passes (DAG => converges); cycles (shouldn't exist) are capped.
  for (let i = 0; i < jobIds.length; i++) {
    for (const e of edges) {
      const d = (depth.get(e.upstream) ?? 0) + 1;
      if (d > (depth.get(e.downstream) ?? 0)) depth.set(e.downstream, d);
    }
  }
  const perLevel = new Map<number, number>();
  const pos = new Map<string, { x: number; y: number }>();
  for (const id of jobIds) {
    const lvl = depth.get(id) ?? 0;
    const col = perLevel.get(lvl) ?? 0;
    perLevel.set(lvl, col + 1);
    pos.set(id, { x: lvl * 240, y: col * 90 });
  }
  return pos;
}

export function PipelineView() {
  const navigate = useNavigate();
  const [ctxId, setCtxId] = useState("");
  const [applied, setApplied] = useState("");

  const jobsQ = useQuery({
    queryKey: ["jobs", "ctx", applied],
    queryFn: () => api.listJobs({ ctx_id: applied, limit: 500 }),
    enabled: applied !== "",
  });
  const treeQ = useQuery({
    queryKey: ["tree", applied],
    // Any job in the ctx returns the same context tree; use the first.
    queryFn: () => api.tree(jobsQ.data![0].id),
    enabled: applied !== "" && !!jobsQ.data && jobsQ.data.length > 0,
  });

  const { nodes, edges } = useMemo(() => {
    const jobs = jobsQ.data ?? [];
    const treeEdges = treeQ.data ?? [];
    const pos = layout(
      jobs.map((j) => j.id),
      treeEdges,
    );
    const nodes: Node[] = jobs.map((j) => ({
      id: j.id,
      position: pos.get(j.id) ?? { x: 0, y: 0 },
      data: {
        label:
          j.state === "waiting" && j.wait_key
            ? `${j.task_name}\n⏸ ${j.wait_key}`
            : j.task_name,
      },
      style: {
        backgroundColor: stateColor(j.state),
        color: "#fff",
        border: "1px solid rgba(0,0,0,0.25)",
        borderRadius: 6,
        fontSize: 11,
        width: 160,
        padding: 8,
        whiteSpace: "pre-line" as const,
      },
    }));
    const edges: Edge[] = treeEdges.map((e, i) => ({
      id: `e${i}`,
      source: e.upstream,
      target: e.downstream,
      label: e.alias ?? undefined,
      animated: true,
      style: { stroke: "#4b5563" },
    }));
    return { nodes, edges };
  }, [jobsQ.data, treeQ.data]);

  return (
    <Panel
      title="Pipeline"
      actions={
        <form
          className="flex gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            setApplied(ctxId.trim());
          }}
        >
          <input
            className="w-72 rounded border border-symba-border bg-symba-bg px-2 py-1 text-sm"
            placeholder="ctx_id"
            value={ctxId}
            onChange={(e) => setCtxId(e.target.value)}
          />
          <button className="rounded bg-indigo-600 px-3 py-1 text-sm text-white">Load</button>
        </form>
      }
    >
      {applied === "" ? (
        <div className="p-8 text-center text-sm text-symba-muted">
          Enter a ctx_id to render its job graph.
        </div>
      ) : jobsQ.isLoading ? (
        <Loading />
      ) : jobsQ.error ? (
        <ErrorBox error={jobsQ.error} />
      ) : (
        <div className="h-[32rem] rounded border border-symba-border">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            fitView
            onNodeClick={(_, n) => navigate({ to: "/jobs/$jobId", params: { jobId: n.id } })}
          >
            <Background />
            <Controls />
          </ReactFlow>
        </div>
      )}
    </Panel>
  );
}
