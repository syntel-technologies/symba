-- mark_workers_stale.sql — flag workers whose heartbeat window lapsed.
--
-- A worker that crashed (no clean Claim-stream close) leaves its row behind with a
-- frozen last_seen. The sweeper flags it stale so the fleet view shows a dead worker
-- before its in-flight leases are reclaimed. The engine does not know each worker's
-- own heartbeat cadence, so the caller passes the threshold in seconds
-- (worker_stale_after_heartbeats * worker_heartbeat_interval_s — slot frames
-- double as heartbeats and refresh last_seen via worker_upsert.sql).
--
-- Only flips false -> true (idempotent, cheap): a re-connected worker clears the
-- flag through worker_upsert.sql, not here.
--
-- Transaction context: general pool, runs on the sweeper's election-won connection.
-- Lock behavior: none (single UPDATE). The ix_workers_last_seen index bounds the scan.
--
-- Parameters: $1 int stale_after_s
UPDATE workers
SET stale = true
WHERE stale = false
  AND last_seen < now() - make_interval(secs => $1);
