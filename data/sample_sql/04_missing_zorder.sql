-- Missing Z-Order on large table filter
SELECT id, timestamp, event_type
FROM huge_events_table
WHERE event_type = 'LOGIN';
