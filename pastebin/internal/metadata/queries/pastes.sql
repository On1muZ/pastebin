-- name: CreatePaste :one
INSERT INTO
  pastes (
    id,
    addr,
    hash,
    delete_at,
    delete_after_view,
    size
  )
VALUES
  ($1, $2, $3, $4, $5, $6)
RETURNING
  *;

-- name: GetPasteByAddr :one
SELECT
  *
FROM
  pastes
WHERE
  addr = $1
  AND (
    delete_at >= now()
    OR delete_at IS NULL
  );

-- name: GetPastesToDelete :many
SELECT
  *
FROM
  pastes
WHERE
  delete_at < now() - INTERVAL '1 minute'
ORDER BY
  delete_at ASC
LIMIT
  $1;

-- name: DeletePaste :execrows
DELETE FROM pastes
WHERE
  id = $1;

-- name: IncViews :execrows
UPDATE pastes p
SET
  views = p.views + d.delta
FROM
  (
    SELECT
      unnest(sqlc.arg(ids)::TEXT[]) AS id,
      unnest(sqlc.arg(deltas)::BIGINT[]) AS delta
  ) AS d
WHERE
  p.id = d.id;

-- name: ConsumePasteByAddr :one
UPDATE pastes
SET
  delete_at = now()
WHERE
  addr = $1
  AND delete_after_view
  AND (
    delete_at IS NULL
    OR delete_at > now()
  )
RETURNING
  *;
