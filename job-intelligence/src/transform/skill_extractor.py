# Skill extraction from job descriptions
import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Skill dictionary – multi-word skills checked first, then single-word
# ---------------------------------------------------------------------------
MULTI_WORD_SKILLS = [
    "machine learning", "deep learning", "natural language processing",
    "data engineering", "data science", "data analysis", "data modeling",
    "data warehouse", "data pipeline", "data lake",
    "web development", "software engineering", "software development",
    "cloud computing", "computer vision", "computer science",
    "big data", "ci cd", "ci/cd",
    "power bi", "google cloud", "google cloud platform",
    "amazon web services", "azure devops",
    "project management", "product management",
    "version control", "unit testing", "test driven development",
    "agile methodology", "scrum master",
    "rest api", "restful api", "graphql api",
    "react native", "node js", "vue js", "next js",
    "ruby on rails", "spring boot",
]

SINGLE_WORD_SKILLS = [
    # Programming languages
    "python", "java", "javascript", "typescript", "scala", "kotlin",
    "rust", "golang", "go", "ruby", "php", "swift", "r", "c#", "c++",
    # Data & databases
    "sql", "nosql", "postgresql", "postgres", "mysql", "mongodb",
    "redis", "elasticsearch", "cassandra", "dynamodb", "sqlite",
    "snowflake", "redshift", "bigquery",
    # Frameworks & libraries
    "django", "flask", "fastapi", "react", "angular", "vue",
    "spring", "express", "pandas", "numpy", "scipy",
    "tensorflow", "pytorch", "keras", "scikit-learn", "sklearn",
    "spark", "hadoop", "airflow", "dbt", "kafka", "flink",
    "streamlit", "plotly", "matplotlib",
    # Cloud & infra
    "aws", "azure", "gcp", "docker", "kubernetes", "terraform",
    "ansible", "jenkins", "github", "gitlab", "linux", "bash",
    # Tools & concepts
    "git", "jira", "confluence", "grafana", "prometheus",
    "tableau", "looker", "excel",
    "graphql", "rest", "api", "microservices", "etl", "elt",
    "devops", "mlops", "dataops",
    "agile", "scrum", "kanban",
]


def _normalize_text(text: str) -> str:
    """Lowercase and normalize whitespace for matching."""
    text = text.lower()
    text = re.sub(r"[^\w\s/#+\-]", " ", text)  # keep / # + - for skills like c#, ci/cd, scikit-learn
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_skills(description: Optional[str]) -> list:
    """
    Extract technical skills from a job description.
    Returns a sorted, deduplicated list of skill names (lowercase).
    """
    if not description:
        return []

    text = _normalize_text(description)
    found_skills = set()

    # Check multi-word skills first (longer phrases matched before single words)
    for skill in MULTI_WORD_SKILLS:
        pattern = r"\b" + re.escape(skill) + r"s?\b"
        if re.search(pattern, text):
            found_skills.add(skill)

    # Check single-word skills
    for skill in SINGLE_WORD_SKILLS:
        pattern = r"\b" + re.escape(skill) + r"s?\b"
        if re.search(pattern, text):
            found_skills.add(skill)

    return sorted(found_skills)


def extract_skills_batch(jobs: list) -> dict:
    """
    Extract skills from a batch of job records.
    Each job should have a 'description' key.

    Returns:
        {
            "job_skills": {job_id: [skill1, skill2, ...], ...},
            "skill_counts": {skill_name: count, ...},
        }
    """
    job_skills = {}
    skill_counts = {}

    for job in jobs:
        job_id = str(job.get("id", ""))
        description = job.get("description", "")
        skills = extract_skills(description)

        job_skills[job_id] = skills

        for skill in skills:
            skill_counts[skill] = skill_counts.get(skill, 0) + 1

    # Sort skill_counts by frequency descending
    skill_counts = dict(sorted(skill_counts.items(), key=lambda x: x[1], reverse=True))

    logger.info(f"Extracted skills from {len(jobs)} jobs, found {len(skill_counts)} unique skills")
    return {
        "job_skills": job_skills,
        "skill_counts": skill_counts,
    }


def get_top_skills(skill_counts: dict, top_n: int = 20) -> list:
    """Return the top N skills by frequency as a list of (skill, count) tuples."""
    return list(skill_counts.items())[:top_n]
