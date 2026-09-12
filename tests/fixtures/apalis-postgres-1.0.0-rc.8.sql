-- Released apalis-postgres 1.0.0-rc.8, all 19 upstream migrations in order.
-- Source: https://crates.io/crates/apalis-postgres/1.0.0-rc.8
-- MIT: see apalis-postgres-LICENSE. Do not derive this fixture from our migrations.

-- 20220530084123_jobs_workers.sql
    CREATE SCHEMA apalis;

    CREATE TABLE IF NOT EXISTS apalis.workers (
        id TEXT NOT NULL,
        worker_type TEXT NOT NULL,
        storage_name TEXT NOT NULL,
        layers TEXT NOT NULL DEFAULT '',
        last_seen timestamptz not null default now()
    );

    CREATE INDEX IF NOT EXISTS Idx ON apalis.workers(id);

    CREATE UNIQUE INDEX IF NOT EXISTS unique_worker_id ON apalis.workers (id);

    CREATE INDEX IF NOT EXISTS WTIdx ON apalis.workers(worker_type);

    CREATE INDEX IF NOT EXISTS LSIdx ON apalis.workers(last_seen);

    CREATE TABLE IF NOT EXISTS apalis.jobs (
        job JSONB NOT NULL,
        id TEXT NOT NULL,
        job_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'Pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 25,
        run_at timestamptz NOT NULL default now(),
        last_error TEXT,
        lock_at timestamptz,
        lock_by TEXT,
        done_at timestamptz,
        CONSTRAINT fk_worker_lock_by FOREIGN KEY(lock_by) REFERENCES apalis.workers(id)
    );

    CREATE INDEX IF NOT EXISTS TIdx ON apalis.jobs(id);

    CREATE INDEX IF NOT EXISTS SIdx ON apalis.jobs(status);

    CREATE UNIQUE INDEX IF NOT EXISTS unique_job_id ON apalis.jobs (id);

    CREATE INDEX IF NOT EXISTS LIdx ON apalis.jobs(lock_by);

    CREATE INDEX IF NOT EXISTS JTIdx ON apalis.jobs(job_type);

    CREATE OR replace FUNCTION apalis.get_job(
                worker_id TEXT,
                v_job_type TEXT
            ) returns apalis.jobs AS $$
            DECLARE
                v_job_id text;
                v_job_row apalis.jobs;
            BEGIN
                SELECT   id, job_type
                INTO     v_job_id, v_job_type
                FROM     apalis.jobs
                WHERE    status = 'Pending'
                AND      run_at < now()
                AND      job_type = v_job_type
                ORDER BY run_at ASC limit 1 FOR UPDATE skip LOCKED;

                IF v_job_id IS NULL THEN
                    RETURN NULL;
                END IF;

                UPDATE apalis.jobs
                    SET
                        status = 'Running',
                        lock_by = worker_id,
                        lock_at = now()
                    WHERE     id = v_job_id
                returning * INTO  v_job_row;
                RETURN v_job_row;
        END;
        $$ LANGUAGE plpgsql volatile;

        CREATE FUNCTION apalis.notify_new_jobs() returns trigger as $$
            BEGIN
                 perform pg_notify('apalis::job', 'insert');
                 return new;
            END;
        $$ language plpgsql;

        CREATE TRIGGER notify_workers after insert on apalis.jobs for each statement execute procedure apalis.notify_new_jobs();



-- 20220709210445_add_job_fn.sql
CREATE OR REPLACE FUNCTION apalis.push_job(
    job_type text,
    job json DEFAULT NULL :: json,
    job_id text DEFAULT NULL :: text,
    status text DEFAULT 'Pending' :: text,
    run_at timestamptz DEFAULT NOW() :: timestamptz,
    max_attempts integer DEFAULT 25 :: integer
) RETURNS apalis.jobs AS $$

        DECLARE
            v_job_row apalis.jobs;
            v_job_id text;

        BEGIN
        IF job_type is not NULL and length(job_type) > 512 THEN raise exception 'Job_type is too long (max length: 512).' USING errcode = 'APAJT';
        END IF;

        IF max_attempts < 1 THEN raise exception 'Job maximum attempts must be at least 1.' USING errcode = 'APAMA';
        end IF;

        SELECT
            uuid_in(
                md5(random() :: text || now() :: text) :: cstring
            ) INTO v_job_id;
        INSERT INTO
            apalis.jobs
        VALUES
            (
                job,
                v_job_id,
                job_type,
                status,
                0,
                max_attempts,
                run_at,
                NULL,
                NULL,
                NULL,
                NULL
            )
            returning * INTO v_job_row;
            RETURN v_job_row;
END;
$$ LANGUAGE plpgsql volatile;




-- 20230330210841_replace_add_job_fn.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION  generate_ulid()
RETURNS TEXT
AS $$
DECLARE
  -- Crockford's Base32
  encoding   BYTEA = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  timestamp  BYTEA = E'\\000\\000\\000\\000\\000\\000';
  output     TEXT = '';

  unix_time  BIGINT;
  ulid       BYTEA;
BEGIN
  -- 6 timestamp bytes
  unix_time = (EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) * 1000)::BIGINT;
  timestamp = SET_BYTE(timestamp, 0, (unix_time >> 40)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 1, (unix_time >> 32)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 2, (unix_time >> 24)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 3, (unix_time >> 16)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 4, (unix_time >> 8)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 5, unix_time::BIT(8)::INTEGER);

  -- 10 entropy bytes
  ulid = timestamp || gen_random_bytes(10);

  -- Encode the timestamp
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 0) & 224) >> 5));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 0) & 31)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 1) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 1) & 7) << 2) | ((GET_BYTE(ulid, 2) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 2) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 2) & 1) << 4) | ((GET_BYTE(ulid, 3) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 3) & 15) << 1) | ((GET_BYTE(ulid, 4) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 4) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 4) & 3) << 3) | ((GET_BYTE(ulid, 5) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 5) & 31)));

  -- Encode the entropy
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 6) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 6) & 7) << 2) | ((GET_BYTE(ulid, 7) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 7) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 7) & 1) << 4) | ((GET_BYTE(ulid, 8) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 8) & 15) << 1) | ((GET_BYTE(ulid, 9) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 9) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 9) & 3) << 3) | ((GET_BYTE(ulid, 10) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 10) & 31)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 11) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 11) & 7) << 2) | ((GET_BYTE(ulid, 12) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 12) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 12) & 1) << 4) | ((GET_BYTE(ulid, 13) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 13) & 15) << 1) | ((GET_BYTE(ulid, 14) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 14) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 14) & 3) << 3) | ((GET_BYTE(ulid, 15) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 15) & 31)));

  RETURN output;
END
$$
LANGUAGE plpgsql
VOLATILE;


CREATE OR REPLACE FUNCTION apalis.push_job(
    job_type text,
    job json DEFAULT NULL :: json,
    status text DEFAULT 'Pending' :: text,
    run_at timestamptz DEFAULT NOW() :: timestamptz,
    max_attempts integer DEFAULT 25 :: integer
) RETURNS apalis.jobs AS $$

        DECLARE
            v_job_row apalis.jobs;
            v_job_id text;

        BEGIN
        IF job_type is not NULL and length(job_type) > 512 THEN raise exception 'Job_type is too long (max length: 512).' USING errcode = 'APAJT';
        END IF;

        IF max_attempts < 1 THEN raise exception 'Job maximum attempts must be at least 1.' USING errcode = 'APAMA';
        end IF;

        SELECT
            CONCAT('JID-' || generate_ulid()) INTO v_job_id;
        INSERT INTO
            apalis.jobs
        VALUES
            (
                job,
                v_job_id,
                job_type,
                status,
                0,
                max_attempts,
                run_at,
                NULL,
                NULL,
                NULL,
                NULL
            )
            returning * INTO v_job_row;
            RETURN v_job_row;
END;
$$ LANGUAGE plpgsql volatile;




-- 20230408110421_drop_old_push_job.sql
DROP FUNCTION IF EXISTS apalis.push_job(
    job_type text,
    job json,
    job_id  text,
    status  text,
    run_at timestamptz,
    max_attempts integer
);

-- 20230408234928_add_get_jobs_fn_.sql
DROP FUNCTION apalis.get_job(
        worker_id TEXT,
        v_job_type TEXT
    );

CREATE OR replace FUNCTION apalis.get_jobs(
        worker_id TEXT,
        v_job_type TEXT,
        v_job_count integer DEFAULT 5 :: integer
    ) returns setof apalis.jobs AS $$ BEGIN RETURN QUERY
UPDATE apalis.jobs
SET status = 'Running',
    lock_by = worker_id,
    lock_at = now()
WHERE id IN (
        SELECT id
        FROM apalis.jobs
        WHERE status = 'Pending'
            AND run_at < now()
            AND job_type = v_job_type
        ORDER BY run_at ASC
        limit v_job_count FOR
        UPDATE skip LOCKED
    )
returning *;
END;
$$ LANGUAGE plpgsql volatile;

-- 20240225141841_replace_add_job_fn_remove_jid.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION  generate_ulid()
RETURNS TEXT
AS $$
DECLARE
  -- Crockford's Base32
  encoding   BYTEA = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  timestamp  BYTEA = E'\\000\\000\\000\\000\\000\\000';
  output     TEXT = '';

  unix_time  BIGINT;
  ulid       BYTEA;
BEGIN
  -- 6 timestamp bytes
  unix_time = (EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) * 1000)::BIGINT;
  timestamp = SET_BYTE(timestamp, 0, (unix_time >> 40)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 1, (unix_time >> 32)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 2, (unix_time >> 24)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 3, (unix_time >> 16)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 4, (unix_time >> 8)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 5, unix_time::BIT(8)::INTEGER);

  -- 10 entropy bytes
  ulid = timestamp || gen_random_bytes(10);

  -- Encode the timestamp
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 0) & 224) >> 5));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 0) & 31)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 1) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 1) & 7) << 2) | ((GET_BYTE(ulid, 2) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 2) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 2) & 1) << 4) | ((GET_BYTE(ulid, 3) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 3) & 15) << 1) | ((GET_BYTE(ulid, 4) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 4) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 4) & 3) << 3) | ((GET_BYTE(ulid, 5) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 5) & 31)));

  -- Encode the entropy
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 6) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 6) & 7) << 2) | ((GET_BYTE(ulid, 7) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 7) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 7) & 1) << 4) | ((GET_BYTE(ulid, 8) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 8) & 15) << 1) | ((GET_BYTE(ulid, 9) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 9) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 9) & 3) << 3) | ((GET_BYTE(ulid, 10) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 10) & 31)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 11) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 11) & 7) << 2) | ((GET_BYTE(ulid, 12) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 12) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 12) & 1) << 4) | ((GET_BYTE(ulid, 13) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 13) & 15) << 1) | ((GET_BYTE(ulid, 14) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 14) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 14) & 3) << 3) | ((GET_BYTE(ulid, 15) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 15) & 31)));

  RETURN output;
END
$$
LANGUAGE plpgsql
VOLATILE;


CREATE OR REPLACE FUNCTION apalis.push_job(
    job_type text,
    job json DEFAULT NULL :: json,
    status text DEFAULT 'Pending' :: text,
    run_at timestamptz DEFAULT NOW() :: timestamptz,
    max_attempts integer DEFAULT 25 :: integer
) RETURNS apalis.jobs AS $$

        DECLARE
            v_job_row apalis.jobs;
            v_job_id text;

        BEGIN
        IF job_type is not NULL and length(job_type) > 512 THEN raise exception 'Job_type is too long (max length: 512).' USING errcode = 'APAJT';
        END IF;

        IF max_attempts < 1 THEN raise exception 'Job maximum attempts must be at least 1.' USING errcode = 'APAMA';
        end IF;

        SELECT
            generate_ulid() INTO v_job_id;
        INSERT INTO
            apalis.jobs
        VALUES
            (
                job,
                v_job_id,
                job_type,
                status,
                0,
                max_attempts,
                run_at,
                NULL,
                NULL,
                NULL,
                NULL
            )
            returning * INTO v_job_row;
            RETURN v_job_row;
END;
$$ LANGUAGE plpgsql volatile;




-- 20250210092135_include_failed_in_get_jobs.sql
DROP FUNCTION apalis.get_jobs(
        worker_id TEXT,
        v_job_type TEXT,
         v_job_count integer
    );

CREATE OR REPLACE FUNCTION apalis.get_jobs(
        worker_id TEXT,
        v_job_type TEXT,
        v_job_count integer DEFAULT 5 :: integer
    ) returns setof apalis.jobs AS $$ BEGIN RETURN QUERY
UPDATE apalis.jobs
SET status = 'Running',
    lock_by = worker_id,
    lock_at = now()
WHERE id IN (
        SELECT id
        FROM apalis.jobs
        WHERE status='Pending' OR (status = 'Failed' AND attempts < max_attempts)
            AND run_at < now()
            AND job_type = v_job_type
        ORDER BY run_at ASC
        limit v_job_count FOR
        UPDATE skip LOCKED
    )
returning *;
END;
$$ LANGUAGE plpgsql volatile;


-- 20250223193249_fix_get_jobs_conditional.sql
DROP FUNCTION apalis.get_jobs(
        worker_id TEXT,
        v_job_type TEXT,
        v_job_count integer
    );

CREATE OR REPLACE FUNCTION apalis.get_jobs(
        worker_id TEXT,
        v_job_type TEXT,
        v_job_count integer DEFAULT 5 :: integer
    ) RETURNS setof apalis.jobs AS $$ BEGIN RETURN QUERY
UPDATE apalis.jobs
SET status = 'Running',
    lock_by = worker_id,
    lock_at = now()
WHERE id IN (
        SELECT id
        FROM apalis.jobs
        WHERE (status='Pending' OR (status = 'Failed' AND attempts < max_attempts))
            AND run_at < now()
            AND job_type = v_job_type
        ORDER BY run_at ASC
        LIMIT v_job_count FOR
        UPDATE SKIP LOCKED
    )
returning *;
END;
$$ LANGUAGE plpgsql volatile;


-- 20250307001101_add_job_priority.sql
ALTER TABLE apalis.jobs
ADD COLUMN priority INTEGER DEFAULT 0;

DROP FUNCTION apalis.get_jobs(
        worker_id TEXT,
        v_job_type TEXT,
        v_job_count integer
    );

CREATE OR REPLACE FUNCTION apalis.get_jobs(
        worker_id TEXT,
        v_job_type TEXT,
        v_job_count integer DEFAULT 5 :: integer
    ) RETURNS setof apalis.jobs AS $$ BEGIN RETURN QUERY
UPDATE apalis.jobs
SET status = 'Running',
    lock_by = worker_id,
    lock_at = now()
WHERE id IN (
        SELECT id
        FROM apalis.jobs
        WHERE (status='Pending' OR (status = 'Failed' AND attempts < max_attempts))
            AND run_at < now()
            AND job_type = v_job_type
        ORDER BY priority DESC, run_at ASC
        LIMIT v_job_count FOR
        UPDATE SKIP LOCKED
    )
returning *;
END;
$$ LANGUAGE plpgsql volatile;

CREATE OR REPLACE FUNCTION apalis.push_job(
    job_type text,
    job json DEFAULT NULL :: json,
    status text DEFAULT 'Pending' :: text,
    run_at timestamptz DEFAULT NOW() :: timestamptz,
    max_attempts integer DEFAULT 25 :: integer,
    priority integer DEFAULT 0 :: integer
) RETURNS apalis.jobs AS $$

        DECLARE
            v_job_row apalis.jobs;
            v_job_id text;

        BEGIN
        IF job_type is not NULL and length(job_type) > 512 THEN raise exception 'Job_type is too long (max length: 512).' USING errcode = 'APAJT';
        END IF;

        IF max_attempts < 1 THEN raise exception 'Job maximum attempts must be at least 1.' USING errcode = 'APAMA';
        end IF;

        SELECT
            generate_ulid() INTO v_job_id;
        INSERT INTO
            apalis.jobs
        VALUES
            (
                job,
                v_job_id,
                job_type,
                status,
                0,
                max_attempts,
                run_at,
                NULL,
                NULL,
                NULL,
                NULL,
                priority
            )
            returning * INTO v_job_row;
            RETURN v_job_row;
END;
$$ LANGUAGE plpgsql volatile;


-- 20250404160441_cleanup_old_push_jobs_fn.sql
DROP FUNCTION IF EXISTS apalis.push_job(
    job_type text,
    job json,
    status text,
    run_at timestamptz,
    max_attempts integer
);


-- 20250722071207_improve_notify.sql
DROP TRIGGER IF EXISTS notify_workers ON apalis.jobs;
DROP FUNCTION IF EXISTS apalis.notify_new_jobs;

CREATE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.run_at <= now() THEN
        PERFORM pg_notify(
            'apalis::job::insert',
            json_build_object(
                'job_type', NEW.job_type,
                'id', NEW.id
            )::text
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_workers
AFTER INSERT ON apalis.jobs
FOR EACH ROW EXECUTE FUNCTION apalis.notify_new_jobs();


-- 20251018164839_update_result.sql
ALTER TABLE apalis.jobs
RENAME COLUMN last_error TO last_result;

ALTER TABLE apalis.jobs
    ALTER COLUMN last_result
SET DATA TYPE jsonb
USING last_result::jsonb;


-- 20251018164912_add_metadata.sql
ALTER TABLE
    apalis.jobs
ADD
    COLUMN metadata jsonb;


-- 20251018165007_move_to_bytes.sql
ALTER TABLE
    apalis.jobs
ALTER COLUMN
    job TYPE bytea USING convert_to(job::text, 'UTF8');


-- 20251018165033_add_started_at.sql
ALTER TABLE
    apalis.workers
ADD
    COLUMN started_at TIMESTAMP WITH TIME ZONE;


-- 20251018165056_queue_jobs.sql
DROP FUNCTION apalis.get_jobs(
        worker_id TEXT,
        v_job_type TEXT,
        v_job_count integer
    );

CREATE OR REPLACE FUNCTION apalis.get_jobs(
        worker_id TEXT,
        v_job_type TEXT,
        v_job_count integer DEFAULT 5 :: integer
    ) RETURNS setof apalis.jobs AS $$ BEGIN RETURN QUERY
UPDATE apalis.jobs
SET status = 'Queued',
    lock_by = worker_id,
    lock_at = now()
WHERE id IN (
        SELECT id
        FROM apalis.jobs
        WHERE (status='Pending' OR (status = 'Failed' AND attempts < max_attempts))
            AND run_at < now()
            AND job_type = v_job_type
        ORDER BY priority DESC, run_at ASC
        LIMIT v_job_count FOR
        UPDATE SKIP LOCKED
    )
returning *;
END;
$$ LANGUAGE plpgsql volatile;


-- 20251018165121_notify_run_at.sql
DROP TRIGGER IF EXISTS notify_workers ON apalis.jobs;
DROP FUNCTION IF EXISTS apalis.notify_new_jobs;

CREATE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.run_at <= now() THEN
        PERFORM pg_notify(
            'apalis::job::insert',
            json_build_object(
                'job_type', NEW.job_type,
                'id', NEW.id,
                'run_at', NEW.run_at
            )::text
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_workers
AFTER INSERT ON apalis.jobs
FOR EACH ROW EXECUTE FUNCTION apalis.notify_new_jobs();


-- 20251225090252_include_primary_keys.sql
ALTER TABLE apalis.jobs ADD PRIMARY KEY (id);
ALTER TABLE apalis.workers ADD PRIMARY KEY (id);


-- 20260508093314_idempotency_key.sql
ALTER TABLE
    apalis.jobs
ADD
    COLUMN idempotency_key TEXT;

CREATE UNIQUE INDEX idx_jobs_idempotency_key ON apalis.jobs(job_type, idempotency_key);
