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
  updated_at = now(),
  checked_at = NULL
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
UPDATE blobs
SET
  checked_at = now()
WHERE
  id IN (
    SELECT
      id
    FROM
      blobs
    WHERE
      status = 'deleting'
    ORDER BY
      checked_at NULLS FIRST
    LIMIT
      $1
    FOR UPDATE
      SKIP LOCKED
  )
RETURNING
  *;

-- name: ListUnderReplicated :many
UPDATE blobs
SET
  checked_at = now()
WHERE
  id IN (
    SELECT
      b.id
    FROM
      blobs AS b
    WHERE
      b.status = 'committed'
      AND (
        SELECT
          count(*)
        FROM
          replicas AS r
        WHERE
          r.blob_id = b.id
          AND r.status = 'committed'
      ) < sqlc.arg(want)::INT
    ORDER BY
      b.checked_at NULLS FIRST
    LIMIT
      sqlc.arg(lim)
    FOR UPDATE
      SKIP LOCKED
  )
RETURNING
  *;

-- name: SetLocalAsyncCommit :exec
SET
  LOCAL synchronous_commit = off;
