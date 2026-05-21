DROP TRIGGER IF EXISTS notify_workers ON apalis.jobs;
DROP FUNCTION IF EXISTS apalis.notify_new_jobs;
DROP FUNCTION IF EXISTS apalis.get_jobs(TEXT, TEXT, INTEGER);
DROP TABLE IF EXISTS apalis.jobs;
DROP TABLE IF EXISTS apalis.workers;
DROP SCHEMA IF EXISTS apalis;
