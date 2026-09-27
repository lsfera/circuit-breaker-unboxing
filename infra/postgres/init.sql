-- The consumer application's ledger (packages/consumer). Run once by the postgres image, on an empty volume.
CREATE ROLE consumer LOGIN PASSWORD 'consumer';
-- Contention surfaces as LockTimeoutError, which the application classifies as throttled, instead of a write that
-- hangs until the action's timeout and reads as the database failing.
ALTER ROLE consumer SET lock_timeout = '500ms';
CREATE DATABASE ledger OWNER consumer;

\connect ledger consumer

-- message_id is the producer's idempotency key: a redelivered message writes nothing new.
CREATE TABLE payments (
  message_id  text PRIMARY KEY,
  api_id      text NOT NULL,
  n           integer NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE refunds (
  message_id  text PRIMARY KEY,
  api_id      text NOT NULL,
  n           integer NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now()
);

-- For infra/chaos-app.mjs, which reads one run's rows by `message_id LIKE '<run>:%'`. The primary key's btree
-- follows the database collation, which a LIKE prefix cannot use; without these the read scans the whole ledger.
CREATE INDEX payments_message_id_prefix ON payments (message_id text_pattern_ops);
CREATE INDEX refunds_message_id_prefix ON refunds (message_id text_pattern_ops);
