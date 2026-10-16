-- name: UpsertNode :exec
INSERT INTO
  nodes (id, addr)
VALUES
  ($1, $2)
ON CONFLICT (id) DO UPDATE
SET
  addr = excluded.addr;
