-- +goose Up
CREATE TABLE IF NOT EXISTS nodes (
  id TEXT PRIMARY KEY NOT NULL,
  addr TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS blobs (
  id TEXT PRIMARY KEY NOT NULL CHECK (id ~ '^[0-9a-f]{32}$'),
  size BIGINT NOT NULL,
  hash VARCHAR(64),
  status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'committed', 'deleting')),
  checked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS replicas (
  blob_id TEXT NOT NULL REFERENCES blobs (id) ON DELETE CASCADE,
  node_id TEXT NOT NULL REFERENCES nodes (id),
  status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'committed', 'deleting')),
  checked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (blob_id, node_id)
);

CREATE INDEX IF NOT EXISTS replicas_node_id_idx ON replicas (node_id);

CREATE INDEX IF NOT EXISTS blobs_deleting_idx ON blobs (checked_at)
WHERE
  status = 'deleting';

CREATE INDEX IF NOT EXISTS replicas_pending_idx ON replicas (checked_at)
WHERE
  status = 'pending';

-- +goose Down
DROP INDEX IF EXISTS replicas_pending_idx;

DROP INDEX IF EXISTS blobs_deleting_idx;

DROP TABLE IF EXISTS replicas;

DROP TABLE IF EXISTS nodes;

DROP TABLE IF EXISTS blobs;
