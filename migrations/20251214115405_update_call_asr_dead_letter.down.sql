-- reverse: modify "call_asr_dl" table
ALTER TABLE "call_asr_dl" DROP COLUMN "last_retry_at", DROP COLUMN "retry_count", ADD COLUMN "updated_at" timestamptz NULL;
