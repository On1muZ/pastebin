-- name: AddReplica :execrows
INSERT INTO
  replicas (blob_id, node_id)
SELECT
  id,
  sqlc.arg(node_id)
FROM
  blobs
WHERE
  id = sqlc.arg(blob_id)
  AND status <> 'deleting'
ON CONFLICT DO NOTHING;

-- name: GetReplica :one
SELECT
  *
FROM
  replicas
WHERE
  blob_id = $1
  AND node_id = $2;

-- name: CommitReplica :execrows
UPDATE replicas
SET
  status = 'committed',
  updated_at = now()
WHERE
  blob_id = $1
  AND node_id = $2
  AND status = 'pending';

-- name: GetReplicas :many
SELECT
  *
FROM
  replicas
WHERE
  blob_id = $1;

-- name: MarkReplicasDeleting :exec
UPDATE replicas
SET
  status = 'deleting',
  updated_at = now()
WHERE
  blob_id = $1;

-- name: DeleteReplica :execrows
DELETE FROM replicas
WHERE
  blob_id = $1
  AND node_id = $2;

-- name: AddReplicas :execrows
INSERT INTO
  replicas (blob_id, node_id)
SELECT
  b.id,
  n.node_id
FROM
  blobs AS b
  CROSS JOIN unnest(sqlc.arg(node_ids)::text[]) AS n (node_id)
WHERE
  b.id = sqlc.arg(blob_id)
  AND b.status <> 'deleting'
ON CONFLICT DO NOTHING;

-- name: CommitReplicas :exec
UPDATE replicas
SET
  status = 'committed',
  updated_at = now()
WHERE
  blob_id = sqlc.arg(blob_id)
  AND node_id = ANY (sqlc.arg(node_ids)::text[])
  AND status = 'pending';

-- name: DeletePendingReplicas :exec
DELETE FROM replicas
WHERE
  blob_id = sqlc.arg(blob_id)
  AND node_id = ANY (sqlc.arg(node_ids)::text[])
  AND status = 'pending';

-- name: GetCommittedReplicas :many
SELECT
  sqlc.embed(r)
FROM
  replicas AS r
  JOIN blobs AS b ON b.id = r.blob_id
WHERE
  r.blob_id = $1
  AND b.status = 'committed'
  AND r.status = 'committed';
