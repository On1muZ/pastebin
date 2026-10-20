-- +goose Up
CREATE TABLE IF NOT EXISTS pastes (
  id TEXT PRIMARY KEY,
  addr TEXT UNIQUE NOT NULL,
  hash VARCHAR(64) NOT NULL,
  delete_at TIMESTAMPTZ,
  views BIGINT NOT NULL DEFAULT 0,
  delete_after_view BOOLEAN DEFAULT FALSE NOT NULL,
  size BIGINT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE INDEX paste_delete_at_idx ON pastes (delete_at)
WHERE
  delete_at IS NOT NULL;

-- +goose Down
DROP TABLE IF EXISTS pastes;
