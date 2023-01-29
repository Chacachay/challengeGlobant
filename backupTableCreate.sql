CREATE TABLE IF NOT EXISTS hired_employees_backup (id INTEGER PRIMARY KEY,name text,datetime text,department_id INTEGER,job_id INTEGER);
CREATE TABLE IF NOT EXISTS departments_backup (id INTEGER PRIMARY KEY, department text);
CREATE TABLE IF NOT EXISTS jobs_backup (id INTEGER PRIMARY KEY, job text)