# Streamlit dashboard for job market analytics
import streamlit as st
import pandas as pd

from src.analytics.reports import (
    get_top_companies,
    get_jobs_by_location,
    get_salary_stats,
    get_salary_by_category,
    get_jobs_over_time,
    get_contract_type_breakdown,
    get_pipeline_history,
)
from src.database_connections.db_utils import get_job_count, get_recent_jobs


def main():
    st.set_page_config(page_title="Job Intelligence Dashboard", layout="wide")
    st.title("Job Intelligence Dashboard")
    st.markdown("Real-time insights from the UK tech job market")

    # -----------------------------------------------------------------------
    # Top-level metrics
    # -----------------------------------------------------------------------
    total_jobs = get_job_count()
    salary_stats = get_salary_stats()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Jobs", f"{total_jobs:,}")
    col2.metric("With Salary Data", f"{salary_stats['total_with_salary']:,}")
    col3.metric("Avg Min Salary", f"£{salary_stats['avg_min']:,.0f}" if salary_stats["avg_min"] else "N/A")
    col4.metric("Avg Max Salary", f"£{salary_stats['avg_max']:,.0f}" if salary_stats["avg_max"] else "N/A")

    st.divider()

    # -----------------------------------------------------------------------
    # Two-column layout: companies + locations
    # -----------------------------------------------------------------------
    left, right = st.columns(2)

    with left:
        st.subheader("Top Hiring Companies")
        companies = get_top_companies(limit=15)
        if companies:
            df = pd.DataFrame(companies)
            st.bar_chart(df.set_index("company")["job_count"])
        else:
            st.info("No company data available yet.")

    with right:
        st.subheader("Jobs by Location")
        locations = get_jobs_by_location(limit=15)
        if locations:
            df = pd.DataFrame(locations)
            st.bar_chart(df.set_index("location")["job_count"])
        else:
            st.info("No location data available yet.")

    st.divider()

    # -----------------------------------------------------------------------
    # Salary by category
    # -----------------------------------------------------------------------
    st.subheader("Salary Ranges by Category")
    salary_cat = get_salary_by_category()
    if salary_cat:
        df = pd.DataFrame(salary_cat)
        st.dataframe(
            df.rename(columns={
                "category": "Category",
                "job_count": "Jobs",
                "avg_min": "Avg Min (£)",
                "avg_max": "Avg Max (£)",
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No salary data available yet.")

    # -----------------------------------------------------------------------
    # Job posting trends
    # -----------------------------------------------------------------------
    st.subheader("Job Posting Trends")
    interval = st.selectbox("Group by", ["day", "week", "month"], index=0)
    trends = get_jobs_over_time(interval=interval)
    if trends:
        df = pd.DataFrame(trends)
        df["period"] = pd.to_datetime(df["period"])
        st.line_chart(df.set_index("period")["job_count"])
    else:
        st.info("No trend data available yet.")

    st.divider()

    # -----------------------------------------------------------------------
    # Contract types
    # -----------------------------------------------------------------------
    left2, right2 = st.columns(2)

    with left2:
        st.subheader("Contract Types")
        contracts = get_contract_type_breakdown()
        if contracts:
            df = pd.DataFrame(contracts)
            st.bar_chart(df.set_index("contract_type")["job_count"])
        else:
            st.info("No contract data available yet.")

    with right2:
        st.subheader("Recent Jobs")
        recent = get_recent_jobs(limit=10)
        if recent:
            df = pd.DataFrame(recent)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No jobs loaded yet.")

    # -----------------------------------------------------------------------
    # Pipeline run history
    # -----------------------------------------------------------------------
    st.divider()
    st.subheader("Pipeline Run History")
    history = get_pipeline_history(limit=10)
    if history:
        df = pd.DataFrame(history)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No pipeline runs recorded yet.")


if __name__ == "__main__":
    main()
