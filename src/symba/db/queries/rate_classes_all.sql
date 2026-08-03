-- rate_classes_all.sql — load the full rate-class config.
--
-- The RateLimiter caches this in memory and refreshes it on the refill tick so
-- runtime edits via the admin API take effect without a restart. Read-only; runs on
-- the general pool at startup and on each refresh.
SELECT name, capacity, refill_per_s FROM rate_classes;
