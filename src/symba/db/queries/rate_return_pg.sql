-- rate_return_pg.sql — return unused reserved tokens to the PG-fallback bucket.
--
-- The matcher reserves tokens BEFORE the claim query (keeps the SQL sargable), then
-- returns whatever the claim didn't consume (query returned fewer rows than reserved).
-- Returning is a plain top-up, capped at capacity so a race can never overfill.
--
-- Transaction context: general pool, matcher reservation-return step. Lock behavior:
-- single-row UPDATE (implicit row lock).
--
-- Parameters:
--   $1 text   rate_class name
--   $2 int    tokens to return (reserved - consumed, always >= 0)
UPDATE rate_classes
SET tokens = LEAST(capacity, tokens + $2::double precision)
WHERE name = $1;
