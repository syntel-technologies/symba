-- Expose the handlers advertised by each worker independently from routing tags.
-- Old SDKs omit the additive ClaimRequest field and therefore persist an empty list.
ALTER TABLE workers
ADD COLUMN registered_tasks TEXT[] NOT NULL DEFAULT '{}';
