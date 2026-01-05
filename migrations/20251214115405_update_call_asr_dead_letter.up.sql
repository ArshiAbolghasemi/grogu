-- modify "call_asr_dl" table
ALTER TABLE "call_asr_dl" DROP COLUMN "updated_at", ADD COLUMN "retry_count" bigint NOT NULL DEFAULT 0, ADD COLUMN "last_retry_at" timestamp NULL;
