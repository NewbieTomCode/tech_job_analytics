-- ============================================================
-- Job Intelligence Database Schema
-- ============================================================

-- Raw jobs: landing zone for API responses (store everything as-is)
CREATE TABLE IF NOT EXISTS raw_jobs (
    id              SERIAL PRIMARY KEY,
    job_data        JSONB NOT NULL,
    source          VARCHAR(50) DEFAULT 'adzuna',
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- Index on ingested_at for time-based queries
CREATE INDEX IF NOT EXISTS idx_raw_jobs_ingested_at ON raw_jobs (ingested_at);


-- Cleaned / deduplicated jobs
CREATE TABLE IF NOT EXISTS jobs (
    id              SERIAL PRIMARY KEY,
    adzuna_id       VARCHAR(100) UNIQUE NOT NULL,
    url             TEXT,
    url_hash        VARCHAR(64) UNIQUE NOT NULL,
    title           TEXT NOT NULL,
    company         VARCHAR(255),
    location        VARCHAR(255),
    description     TEXT,
    salary_min      NUMERIC(12,2),
    salary_max      NUMERIC(12,2),
    contract_type   VARCHAR(50),
    category        VARCHAR(255),
    posted_date     TIMESTAMP,
    first_seen_at   TIMESTAMP DEFAULT NOW(),
    last_seen_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_url_hash ON jobs (url_hash);
CREATE INDEX IF NOT EXISTS idx_jobs_posted_date ON jobs (posted_date);
CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs (company);
CREATE INDEX IF NOT EXISTS idx_jobs_location ON jobs (location);


-- Skills reference table
CREATE TABLE IF NOT EXISTS skills (
    id              SERIAL PRIMARY KEY,
    skill_name      VARCHAR(100) UNIQUE NOT NULL
);


-- Many-to-many: which jobs mention which skills
CREATE TABLE IF NOT EXISTS job_skills (
    job_id          INT REFERENCES jobs(id) ON DELETE CASCADE,
    skill_id        INT REFERENCES skills(id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, skill_id)
);


-- Scrape run metadata (one row per pipeline execution)
CREATE TABLE IF NOT EXISTS scrape_runs (
    id              SERIAL PRIMARY KEY,
    run_date        TIMESTAMP DEFAULT NOW(),
    jobs_fetched    INT DEFAULT 0,
    new_jobs        INT DEFAULT 0,
    duplicates      INT DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'success',
    error_message   TEXT
);
