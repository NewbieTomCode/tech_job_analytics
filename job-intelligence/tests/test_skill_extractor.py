# Tests for skill extraction
import pytest
from src.transform.skill_extractor import (
    _normalize_text,
    extract_skills,
    extract_skills_batch,
    get_top_skills,
)


# ---------------------------------------------------------------------------
# _normalize_text
# ---------------------------------------------------------------------------
class TestNormalizeText:
    def test_lowercases(self):
        assert _normalize_text("Python SQL Docker") == "python sql docker"

    def test_collapses_whitespace(self):
        assert _normalize_text("python   sql") == "python sql"

    def test_preserves_slash_for_cicd(self):
        result = _normalize_text("CI/CD pipeline")
        assert "/" in result

    def test_preserves_hash_for_csharp(self):
        result = _normalize_text("C# developer")
        assert "#" in result

    def test_preserves_plus_for_cpp(self):
        result = _normalize_text("C++ required")
        assert "+" in result


# ---------------------------------------------------------------------------
# extract_skills – single word skills
# ---------------------------------------------------------------------------
class TestExtractSkillsSingle:
    def test_finds_python(self):
        skills = extract_skills("We need someone with Python experience")
        assert "python" in skills

    def test_finds_multiple_skills(self):
        skills = extract_skills("Required: Python, SQL, Docker, and AWS experience")
        assert "python" in skills
        assert "sql" in skills
        assert "docker" in skills
        assert "aws" in skills

    def test_case_insensitive(self):
        skills = extract_skills("PYTHON and SQL and DOCKER")
        assert "python" in skills
        assert "sql" in skills
        assert "docker" in skills

    def test_no_false_positives_for_go(self):
        # "go" should match as a skill in programming context
        skills = extract_skills("Experience with Go and Rust required")
        assert "go" in skills
        assert "rust" in skills

    def test_returns_sorted(self):
        skills = extract_skills("Python SQL Docker AWS")
        assert skills == sorted(skills)

    def test_no_duplicates(self):
        skills = extract_skills("Python Python Python")
        assert skills.count("python") == 1

    def test_empty_description(self):
        assert extract_skills("") == []

    def test_none_description(self):
        assert extract_skills(None) == []

    def test_no_skills_found(self):
        skills = extract_skills("Looking for a great team player with good communication")
        # These words shouldn't match any tech skills
        assert len(skills) == 0

    def test_plural_forms_match(self):
        skills = extract_skills("Build microservices and APIs")
        assert "api" in skills
        assert "microservices" in skills

    def test_skill_in_compound_word_no_false_match(self):
        # "sql" should not match inside "nosql" as a separate skill
        # but "nosql" itself is a valid skill
        skills = extract_skills("Experience with NoSQL databases")
        assert "nosql" in skills

    def test_scikit_learn_variants(self):
        skills = extract_skills("Proficient in scikit-learn and sklearn")
        assert "scikit-learn" in skills
        assert "sklearn" in skills


# ---------------------------------------------------------------------------
# extract_skills – multi-word skills
# ---------------------------------------------------------------------------
class TestExtractSkillsMulti:
    def test_machine_learning(self):
        skills = extract_skills("Experience in machine learning and data science")
        assert "machine learning" in skills
        assert "data science" in skills

    def test_ci_cd(self):
        skills = extract_skills("Must know CI/CD pipelines")
        assert "ci/cd" in skills

    def test_data_engineering(self):
        skills = extract_skills("Data engineering role building data pipelines")
        assert "data engineering" in skills
        assert "data pipeline" in skills

    def test_cloud_platforms(self):
        skills = extract_skills("Google Cloud Platform and Amazon Web Services")
        assert "google cloud platform" in skills
        assert "amazon web services" in skills

    def test_react_native(self):
        skills = extract_skills("Build apps with React Native")
        assert "react native" in skills

    def test_rest_api(self):
        skills = extract_skills("Design and build REST API services")
        assert "rest api" in skills


# ---------------------------------------------------------------------------
# extract_skills – mixed single + multi
# ---------------------------------------------------------------------------
class TestExtractSkillsMixed:
    def test_real_world_description(self):
        desc = """
        We are looking for a Data Engineer with experience in Python, SQL,
        and Apache Spark. You should know Docker, Kubernetes, and AWS.
        Experience with machine learning and data pipeline development
        is a plus. CI/CD knowledge required.
        """
        skills = extract_skills(desc)
        assert "python" in skills
        assert "sql" in skills
        assert "spark" in skills
        assert "docker" in skills
        assert "kubernetes" in skills
        assert "aws" in skills
        assert "machine learning" in skills
        assert "data pipeline" in skills
        assert "ci/cd" in skills

    def test_html_in_description(self):
        desc = "<p>Python</p> <strong>SQL</strong> <li>Docker</li>"
        skills = extract_skills(desc)
        assert "python" in skills
        assert "sql" in skills
        assert "docker" in skills

    def test_very_long_description(self):
        """Ensure extraction works on large text without error."""
        desc = "We need Python and SQL skills. " * 500
        skills = extract_skills(desc)
        assert "python" in skills
        assert "sql" in skills

    def test_special_characters_in_text(self):
        desc = "Skills: Python!!! SQL??? Docker..."
        skills = extract_skills(desc)
        assert "python" in skills
        assert "sql" in skills
        assert "docker" in skills


# ---------------------------------------------------------------------------
# extract_skills_batch
# ---------------------------------------------------------------------------
class TestExtractSkillsBatch:
    def test_batch_extraction(self):
        jobs = [
            {"id": "1", "description": "Python and SQL required"},
            {"id": "2", "description": "Docker and Kubernetes experience"},
            {"id": "3", "description": "Python and Docker skills needed"},
        ]
        result = extract_skills_batch(jobs)

        assert "1" in result["job_skills"]
        assert "2" in result["job_skills"]
        assert "3" in result["job_skills"]

        assert "python" in result["job_skills"]["1"]
        assert "docker" in result["job_skills"]["2"]

        # Python appears in jobs 1 and 3
        assert result["skill_counts"]["python"] == 2
        # Docker appears in jobs 2 and 3
        assert result["skill_counts"]["docker"] == 2

    def test_empty_batch(self):
        result = extract_skills_batch([])
        assert result["job_skills"] == {}
        assert result["skill_counts"] == {}

    def test_no_descriptions(self):
        jobs = [{"id": "1"}, {"id": "2"}]
        result = extract_skills_batch(jobs)
        assert result["job_skills"]["1"] == []
        assert result["job_skills"]["2"] == []

    def test_skill_counts_sorted_descending(self):
        jobs = [
            {"id": "1", "description": "Python SQL Docker"},
            {"id": "2", "description": "Python SQL"},
            {"id": "3", "description": "Python"},
        ]
        result = extract_skills_batch(jobs)
        counts = list(result["skill_counts"].values())
        assert counts == sorted(counts, reverse=True)


# ---------------------------------------------------------------------------
# get_top_skills
# ---------------------------------------------------------------------------
class TestGetTopSkills:
    def test_returns_top_n(self):
        counts = {"python": 10, "sql": 8, "docker": 5, "aws": 3, "java": 1}
        top = get_top_skills(counts, top_n=3)
        assert len(top) == 3
        assert top[0] == ("python", 10)
        assert top[1] == ("sql", 8)
        assert top[2] == ("docker", 5)

    def test_fewer_than_n(self):
        counts = {"python": 5, "sql": 3}
        top = get_top_skills(counts, top_n=10)
        assert len(top) == 2

    def test_empty_counts(self):
        assert get_top_skills({}) == []

    def test_default_top_n(self):
        counts = {f"skill_{i}": i for i in range(30)}
        top = get_top_skills(counts)
        assert len(top) == 20
