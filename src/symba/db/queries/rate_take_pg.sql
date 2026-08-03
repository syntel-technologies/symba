-- rate_take_pg.sql — PG-fallback token bucket refill-and-take.
--
-- This is the degraded-mode path used when Redis is unreachable. It reproduces the
-- Redis Lua semantics exactly, but with Postgres row-level locking as the atom:
--   1. lazily refill: add refill_per_s * elapsed since refilled_at, capped at capacity,
--   2. take up to $2 tokens (never more than are available),
--   3. stamp refilled_at=now() so the NEXT call measures elapsed from here.
--
-- Transaction context: runs on the general pool INSIDE the matcher's reservation
-- step, one row per rate_class. Lock behavior: the implicit row lock on the
-- rate_classes row serializes concurrent engine instances — engine-wide correctness
-- at higher latency than Redis.
--
-- CTE-then-UPDATE (read-only CTE, single UPDATE): a plain modifying CTE that updated
-- the row and a RETURNING that re-derived `granted` from the row is WRONG — in Postgres
-- RETURNING sees the NEW (already-decremented) tokens, so it re-computes granted against
-- the post-take balance and silently under-grants (returns leftover, not taken).
--
-- Instead the `calc` CTE reads the row ONCE, locks it (FOR UPDATE) so concurrent engines
-- serialize, and computes both the refilled balance and the granted amount from the OLD
-- tokens. The UPDATE then writes refilled - granted and RETURNS the pre-computed granted.
--
-- Returns the number of tokens actually granted (0..$2). The matcher treats a grant
-- of 0 as "class exhausted" and pushes the class into the claim exhausted array; unused
-- grants are returned via rate_return_pg.
--
-- Parameters:
--   $1 text   rate_class name
--   $2 int    tokens requested (the batch's LIMIT for that class)
WITH calc AS (
    SELECT
        name,
        LEAST(capacity, tokens + EXTRACT(EPOCH FROM (now() - refilled_at)) * refill_per_s) AS refilled
    FROM rate_classes
    WHERE name = $1
    FOR UPDATE
),
granted AS (
    SELECT name, refilled, floor(LEAST(refilled, $2::double precision)) AS take
    FROM calc
)
UPDATE rate_classes rc
SET tokens = g.refilled - g.take,
    refilled_at = now()
FROM granted g
WHERE rc.name = g.name
RETURNING g.take::int AS granted;
