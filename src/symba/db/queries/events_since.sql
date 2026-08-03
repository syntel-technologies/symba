-- events_since.sql — tail the ledger for the SSE fan-out (backs live UI updates).
--
-- The SSE channel (/v1/events/stream) is fed by polling the ledger for rows newer
-- than the last id we forwarded. The monotonic IDENTITY `id` is the cursor (never
-- `at`, which can tie across rows in the same ms). This is the pragmatic
-- single-engine fan-out: one poller per engine reads new rows every ~500ms and
-- pushes them to all connected SSE clients; multi-engine UIs are eventually
-- consistent within a second via PG (in-process pubsub; multi-engine UIs are
-- eventually-consistent by design). No LISTEN/NOTIFY: it does not survive a failover
-- and silently drops under load, whereas a bounded id-cursor poll never loses a row.
--
-- Tenant-scoped and page-capped so one busy tenant cannot starve the stream.
--
-- Transaction context: general pool, read-only, no lock. BRIN on `at` plus the
-- identity ordering keeps this a cheap tail scan.
--
-- Parameters: $1 bigint after_id (0 for "from now"), $2 text tenant, $3 int limit
SELECT id, job_id, tenant, ctx_id, event, at, detail
FROM job_events
WHERE id > $1 AND tenant = $2
ORDER BY id ASC
LIMIT $3;
