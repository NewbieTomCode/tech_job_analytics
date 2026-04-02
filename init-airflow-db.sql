-- Create a separate database for Airflow metadata
-- This runs before schema.sql (00_ prefix) on first container start
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO job_admin;
