-- rate_class_upsert.sql — create/update a rate-class config (admin API).
--
-- Runtime-editable via the admin control plane; the change takes effect on the next
-- refill tick when the RateLimiter reloads its cache. A new class starts full
-- (tokens = capacity) so it isn't spuriously exhausted the instant it's created.
--
-- Parameters:
--   $1 text    name
--   $2 double  capacity
--   $3 double  refill_per_s
INSERT INTO rate_classes (name, capacity, refill_per_s, tokens, refilled_at, updated_at)
VALUES ($1, $2, $3, $2, now(), now())
ON CONFLICT (name) DO UPDATE
SET capacity = EXCLUDED.capacity,
    refill_per_s = EXCLUDED.refill_per_s,
    updated_at = now();
