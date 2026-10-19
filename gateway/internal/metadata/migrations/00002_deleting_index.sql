-- +goose Up
CREATE INDEX IF NOT EXISTS blobs_deleting_idx ON blobs (updated_at)
WHERE
  status = 'deleting';

-- +goose Down
DROP INDEX IF EXISTS blobs_deleting_idx;
