-- create "call_asr_dl" table
CREATE TABLE "call_asr_dl" (
  "call_id" character varying(255) NOT NULL,
  "msg" jsonb NOT NULL,
  "error" text NOT NULL,
  "status" character varying(20) NOT NULL DEFAULT 'pending',
  "created_at" timestamptz NULL,
  "updated_at" timestamptz NULL,
  PRIMARY KEY ("call_id")
);
