import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ==============================
# Load dataset
# ==============================
@st.cache_data
def load_data():
    df = pd.read_csv("data/clean_osha.csv")
    return df

df = load_data()

# ==============================
# Page config
# ==============================
st.set_page_config(page_title="OSHA Injuries Dashboard", layout="wide")
st.title("📊 OSHA Workplace Injuries Dashboard (CSV Demo)")

# ==============================
# Tabs
# ==============================
tab1, tab2 = st.tabs(["📊 Overview", "🗺️ States"])

# -------------------------------------------------------------------
# TAB 1 - OVERVIEW
# -------------------------------------------------------------------
with tab1:
    st.header("National Overview")

    # KPI by year
    df_kpi = df.groupby("Year").agg({
        "Injuries": "sum",
        "Fatalities": "sum",
        "HoursWorked": "sum",
        "Employees": "sum",
        "DaysAwayFromWork": "sum"
    }).reset_index()

    df_kpi["TRIR"] = (df_kpi["Injuries"] / df_kpi["HoursWorked"]) * 200000
    df_kpi["SeverityRate"] = (df_kpi["DaysAwayFromWork"] / df_kpi["HoursWorked"]) * 200000
    df_kpi["FatalityRate"] = (df_kpi["Fatalities"] / df_kpi["Employees"]) * 100000

    latest = df_kpi.iloc[-1]
    prev = df_kpi.iloc[-2] if len(df_kpi) > 1 else None

    c1, c2, c3 = st.columns(3)
    c1.metric("💥 TRIR", f"{latest['TRIR']:.2f}", f"Δ vs {prev['Year']}: {latest['TRIR'] - prev['TRIR']:+.2f}" if prev is not None else "")
    c2.metric("📆 Severity", f"{latest['SeverityRate']:.2f}", f"Δ vs {prev['Year']}: {latest['SeverityRate'] - prev['SeverityRate']:+.2f}" if prev is not None else "")
    c3.metric("☠️ Fatality Rate", f"{latest['FatalityRate']:.2f}", f"Δ vs {prev['Year']}: {latest['FatalityRate'] - prev['FatalityRate']:+.2f}" if prev is not None else "")

    st.info("Indicators normalized per 200,000 hours or 100,000 employees for comparability.")

    # Trend
    fig = px.line(df_kpi, x="Year", y="Injuries", markers=True,
                  labels={"Year": "Year", "Injuries": "Total Injuries"})
    st.plotly_chart(fig, use_container_width=True)

# -------------------------------------------------------------------
# TAB 2 - STATES
# -------------------------------------------------------------------
with tab2:
    st.header("State Analysis")

    states = sorted(df["StateName"].dropna().unique())
    state_choice = st.selectbox("🗺️ Select a State", states)

    df_state = df[df["StateName"] == state_choice]

    state_summary = df_state.groupby("Year").agg({
        "Injuries": "sum",
        "Fatalities": "sum",
        "DaysAwayFromWork": "sum",
        "HoursWorked": "sum",
        "Employees": "sum"
    }).reset_index()

    state_summary["TRIR"] = (state_summary["Injuries"] / state_summary["HoursWorked"]) * 200000
    state_summary["FatalityRate"] = (state_summary["Fatalities"] / state_summary["Employees"]) * 100000

    st.dataframe(state_summary)

    fig2 = px.line(state_summary, x="Year", y="Injuries", markers=True,
                   labels={"Year": "Year", "Injuries": "Injuries"})
    st.plotly_chart(fig2, use_container_width=True)