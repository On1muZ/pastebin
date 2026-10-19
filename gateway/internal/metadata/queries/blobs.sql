-- name: CreateBlob :execrows
INSERT INTO
  blobs (id, size)
VALUES
  ($1, $2)
ON CONFLICT (id) DO NOTHING;

-- name: GetBlob :one
SELECT
  *
FROM
  blobs
WHERE
  id = $1;

-- name: CommitBlob :execrows
UPDATE blobs
SET
  status = 'committed',
  hash = $1,
  updated_at = now()
WHERE
  id = $2
  AND status = 'pending';

-- name: MarkBlobDeleting :execrows
UPDATE blobs
SET
  status = 'deleting',
  updated_at = now()
WHERE
  id = $1;

-- name: DeleteBlob :execrows
DELETE FROM blobs
WHERE
  id = $1;

-- name: ListDeleting :many
SELECT
  *
FROM
  blobs
WHERE
  status = 'deleting'
ORDER BY
  updated_at
LIMIT
  $1;

-- name: ListUnderReplicated :many
SELECT
  sqlc.embed(b)
FROM
  blobs AS b
  LEFT JOIN replicas AS r ON r.blob_id = b.id
WHERE
  b.status = 'committed'
GROUP BY
  b.id
HAVING
  count(*) FILTER (
    WHERE
      r.status = 'committed'
  ) < sqlc.arg(want)::int
ORDER BY
  b.updated_at
LIMIT
  sqlc.arg(lim);

-- name: SetLocalAsyncCommit :exec
SET LOCAL synchronous_commit = off;
