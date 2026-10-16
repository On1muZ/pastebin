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
