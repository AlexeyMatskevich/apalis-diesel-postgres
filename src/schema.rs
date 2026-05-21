diesel::table! {
    use diesel::sql_types::*;

    apalis.jobs (id) {
        job -> Binary,
        id -> Text,
        job_type -> Text,
        status -> Text,
        attempts -> Int4,
        max_attempts -> Int4,
        run_at -> Timestamptz,
        last_result -> Nullable<Jsonb>,
        lock_at -> Nullable<Timestamptz>,
        lock_by -> Nullable<Text>,
        done_at -> Nullable<Timestamptz>,
        priority -> Int4,
        metadata -> Nullable<Jsonb>,
        idempotency_key -> Nullable<Text>,
    }
}

diesel::table! {
    use diesel::sql_types::*;

    apalis.workers (id, worker_type) {
        id -> Text,
        worker_type -> Text,
        storage_name -> Text,
        layers -> Text,
        last_seen -> Timestamptz,
        started_at -> Nullable<Timestamptz>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(jobs, workers);
