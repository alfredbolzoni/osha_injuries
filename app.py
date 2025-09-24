# app_sql.py — OSHA Workplace Injuries Dashboard (Postgres SQL edition)

import io
import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from psycopg2.extras import RealDictCursor
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

# ==============================
# Page Config
# ==============================
st.set_page_config(page_title="OSHA Workplace Injuries Dashboard", layout="wide")
st.title("📊 OSHA Workplace Injuries Dashboard")

# ==============================
# DB Connection
# ==============================
def run_query(query: str, params=None) -> pd.DataFrame:
    conn = psycopg2.connect(
        host=st.secrets["postgres"]["host"],
        port=st.secrets["postgres"]["port"],
        dbname=st.secrets["postgres"]["dbname"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"],
        sslmode="require"
    )
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query, params or [])
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows)

# ==============================
# Preload Base Tables
# ==============================
@st.cache_data(show_spinner=False, ttl=600)
def load_regions():
    return run_query("SELECT state_code, state_name FROM regions;")

@st.cache_data(show_spinner=False, ttl=600)
def load_sectors():
    return run_query("SELECT naics_code, sector_macro FROM sectors;")

@st.cache_data(show_spinner=False, ttl=600)
def load_incidents():
    return run_query("""
        SELECT year, state_code, naics_code,
               injuries, fatalities, hoursworked,
               employees, daysawayfromwork, jobtransferrestriction
        FROM incidents;
    """)

df_regions = load_regions()
df_sectors = load_sectors()
df_inc = load_incidents()

# Merge helpers
def incidents_with_state():
    return df_inc.merge(df_regions, on="state_code", how="left")

def incidents_with_state_sector():
    return incidents_with_state().merge(df_sectors, on="naics_code", how="left")

# Utils
def safe_div(num, den, factor=1.0, ndigits=2):
    if pd.isna(num) or pd.isna(den) or den == 0:
        return 0.0
    return round((num / den) * factor, ndigits)

# ==============================
# Tabs
# ==============================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Overview", "🗺️ States", "🏭 Sectors", "🔍 Combined Analysis", "💡 Insights & Export"
])

# -------------------------------------------------------------------
# TAB 1 - OVERVIEW
# -------------------------------------------------------------------
with tab1:
    st.header("National Overview")

    # Query diretta ad aggregare lato DB (molto più leggero)
    df_grp = run_query("""
        SELECT year,
               SUM(injuries) AS injuries,
               SUM(fatalities) AS fatalities,
               SUM(hoursworked) AS hoursworked,
               SUM(employees) AS employees,
               SUM(daysawayfromwork) AS daysawayfromwork
        FROM incidents
        GROUP BY year
        ORDER BY year;
    """)

    if df_grp.empty:
        st.error("⚠️ No data available in 'incidents'.")
    else:
        # Calcoli KPI direttamente su df piccolo (6 righe = anni)
        df_grp["TRIR"] = (df_grp["injuries"] / df_grp["hoursworked"]).fillna(0) * 200000
        df_grp["SeverityRate"] = (df_grp["daysawayfromwork"] / df_grp["hoursworked"]).fillna(0) * 200000
        df_grp["FatalityRate"] = (df_grp["fatalities"] / df_grp["employees"]).fillna(0) * 100000

        latest = df_grp.iloc[-1]
        prev = df_grp.iloc[-2] if len(df_grp) > 1 else None

        def delta_str(metric):
            if prev is None:
                return "N/A"
            return f"{latest[metric] - prev[metric]:+.2f}"

        c1, c2, c3 = st.columns(3)
        c1.metric("💥 TRIR", f"{latest['TRIR']:.2f}", f"Δ vs {int(prev['year'])}: {delta_str('TRIR')}" if prev is not None else "")
        c2.metric("📆 Severity Rate", f"{latest['SeverityRate']:.2f}", f"Δ vs {int(prev['year'])}: {delta_str('SeverityRate')}" if prev is not None else "")
        c3.metric("☠️ Fatality Rate", f"{latest['FatalityRate']:.2f}", f"Δ vs {int(prev['year'])}: {delta_str('FatalityRate')}" if prev is not None else "")

        # Trend
        st.subheader("📈 National Injury Trend")
        fig_trend = px.line(df_grp, x="year", y="injuries", markers=True,
                            labels={"year": "Year", "injuries": "Injuries"})
        fig_trend.update_traces(hovertemplate="Year %{x}<br>Injuries: %{y:,}")
        st.plotly_chart(fig_trend, use_container_width=True)

# -------------------------------------------------------------------
# TAB 2 - STATES
# -------------------------------------------------------------------
with tab2:
    st.header("State Analysis")

    states = sorted(df_regions["state_name"].dropna().unique().tolist())
    state_choice = st.selectbox("🗺️ Select a State:", states)

    df_state = incidents_with_state()
    df_state = df_state[df_state["state_name"] == state_choice]

    if not df_state.empty:
        nat_sum = incidents_with_state().sum(numeric_only=True)
        st_sum = df_state.sum(numeric_only=True)

        state_trir = safe_div(st_sum["injuries"], st_sum["hoursworked"], 200000)
        nat_trir   = safe_div(nat_sum["injuries"], nat_sum["hoursworked"], 200000)
        state_sev  = safe_div(st_sum["daysawayfromwork"], st_sum["hoursworked"], 200000)
        nat_sev    = safe_div(nat_sum["daysawayfromwork"], nat_sum["hoursworked"], 200000)
        state_fat  = safe_div(st_sum["fatalities"], st_sum["employees"], 100000)
        nat_fat    = safe_div(nat_sum["fatalities"], nat_sum["employees"], 100000)

        c1, c2, c3 = st.columns(3)
        c1.metric("💥 TRIR", state_trir, f"National: {nat_trir}")
        c2.metric("📆 Severity", state_sev, f"National: {nat_sev}")
        c3.metric("☠️ Fatality Rate", state_fat, f"National: {nat_fat}")

        # Trend
        df_trend = df_state.groupby("year")["injuries"].sum().reset_index()
        st.subheader(f"📈 Injury Trend in {state_choice}")
        fig_state_trend = px.line(df_trend, x="year", y="injuries", markers=True)
        st.plotly_chart(fig_state_trend, use_container_width=True)

# -------------------------------------------------------------------
# TAB 3 - SECTORS
# -------------------------------------------------------------------
with tab3:
    st.header("Sector Analysis")

    macros = sorted(df_sectors["sector_macro"].dropna().unique().tolist())
    sector_choice = st.selectbox("🏭 Select a Macro Sector:", macros)

    df_sec = incidents_with_state_sector()
    df_sec = df_sec[df_sec["sector_macro"] == sector_choice]

    if not df_sec.empty:
        nat_sum = incidents_with_state_sector().sum(numeric_only=True)
        sec_sum = df_sec.sum(numeric_only=True)

        sec_trir = safe_div(sec_sum["injuries"], sec_sum["hoursworked"], 200000)
        nat_trir = safe_div(nat_sum["injuries"], nat_sum["hoursworked"], 200000)

        c1, c2 = st.columns(2)
        c1.metric("💥 TRIR", sec_trir, f"National: {nat_trir}")
        c2.metric("📆 Severity", safe_div(sec_sum["daysawayfromwork"], sec_sum["hoursworked"], 200000),
                  f"National: {safe_div(nat_sum['daysawayfromwork'], nat_sum['hoursworked'], 200000)}")

        # Top risky sub-sectors
        df_sec["naics3"] = df_sec["naics_code"].astype(str).str[:3]
        top_sub = df_sec.groupby("naics3")["injuries"].sum().reset_index().sort_values("injuries", ascending=False).head(10)
        st.subheader(f"🏭 Top 10 Risky Sub-sectors in {sector_choice}")
        fig_sub = px.bar(top_sub, x="naics3", y="injuries")
        st.plotly_chart(fig_sub, use_container_width=True)

# -------------------------------------------------------------------
# TAB 4 - COMBINED ANALYSIS
# -------------------------------------------------------------------
with tab4:
    st.header("Combined Analysis: State + Sector + Year")

    df_all = incidents_with_state_sector()
    years = sorted(df_all["year"].dropna().unique().astype(int).tolist())
    states = sorted(df_all["state_name"].dropna().unique().tolist())
    sectors = sorted(df_all["sector_macro"].dropna().unique().tolist())

    year_sel = st.selectbox("📅 Select Year:", years, index=len(years)-1)
    state_sel = st.selectbox("🗺️ Select State:", states)
    sector_sel = st.selectbox("🏭 Select Macro Sector:", sectors)

    df_f = df_all[(df_all["year"] == year_sel) & 
                  (df_all["state_name"] == state_sel) & 
                  (df_all["sector_macro"] == sector_sel)]

    if not df_f.empty:
        tbl = df_f.agg({"injuries": "sum", "fatalities": "sum", "hoursworked": "sum", "employees": "sum"}).to_frame().T
        tbl["TRIR (/200k hrs)"] = safe_div(tbl.loc[0, "injuries"], tbl.loc[0, "hoursworked"], 200000)

        st.subheader(f"📊 {state_sel} – {sector_sel} ({year_sel})")
        st.dataframe(tbl)

        # Trend multi-year
        df_tr = df_all[(df_all["state_name"] == state_sel) & (df_all["sector_macro"] == sector_sel)].groupby("year")["injuries"].sum().reset_index()
        st.subheader(f"📈 Injury Trend – {state_sel} ({sector_sel})")
        fig_tr = px.line(df_tr, x="year", y="injuries", markers=True)
        st.plotly_chart(fig_tr, use_container_width=True)

        # KPI Gauge
        nat = df_all[df_all["year"] == year_sel]
        val = safe_div(tbl.loc[0, "injuries"], tbl.loc[0, "hoursworked"], 200000)
        ref = safe_div(nat["injuries"].sum(), nat["hoursworked"].sum(), 200000)

        st.subheader("📌 TRIR vs National Average")
        rng = max(val, ref) * 1.5 if max(val, ref) > 0 else 1
        fig_kpi = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=val,
            delta={"reference": ref, "increasing": {"color": "red"}, "decreasing": {"color": "green"}},
            gauge={"axis": {"range": [0, rng]}}
        ))
        st.plotly_chart(fig_kpi, use_container_width=True)

# -------------------------------------------------------------------
# TAB 5 - INSIGHTS & EXPORT
# -------------------------------------------------------------------
with tab5:
    st.header("Insights & Export")

    df_all = incidents_with_state_sector()
    if df_all.empty:
        st.error("⚠️ df_all is empty after merge.")
    else:
        latest_year = df_all["year"].max()

        # Top states
        df_states = (
            df_all[df_all["year"] == latest_year]
            .groupby("state_name")
            .agg(injuries=("injuries", "sum"), hours=("hoursworked", "sum"))
            .reset_index()
        )
        df_states["TRIR"] = df_states.apply(
            lambda r: safe_div(r["injuries"], r["hours"], 200000), axis=1
        )
        df_states = df_states.sort_values("TRIR", ascending=False).head(10)

        st.subheader(f"🔥 Top 10 States by TRIR ({latest_year})")
        fig_s = px.bar(df_states, x="TRIR", y="state_name", orientation="h")
        st.plotly_chart(fig_s, use_container_width=True)

        # Top sectors
        df_secs = (
            df_all[df_all["year"] == latest_year]
            .groupby("sector_macro")
            .agg(injuries=("injuries", "sum"), hours=("hoursworked", "sum"))
            .reset_index()
        )
        df_secs["TRIR"] = df_secs.apply(
            lambda r: safe_div(r["injuries"], r["hours"], 200000), axis=1
        )
        df_secs = df_secs.sort_values("TRIR", ascending=False).head(10)

        st.subheader(f"🏭 Top 10 Sectors by TRIR ({latest_year})")
        fig_m = px.bar(df_secs, x="TRIR", y="sector_macro", orientation="h")
        st.plotly_chart(fig_m, use_container_width=True)

        # Export
        buffer_xlsx = io.BytesIO()
        with pd.ExcelWriter(buffer_xlsx, engine="xlsxwriter") as writer:
            df_all.to_excel(writer, index=False, sheet_name="Incidents")
        st.download_button("⬇️ Download Excel", buffer_xlsx.getvalue(), "hse_report.xlsx")

        buffer_pdf = io.BytesIO()
        doc = SimpleDocTemplate(buffer_pdf, pagesize=A4)
        styles = getSampleStyleSheet()
        elements = [Paragraph("📊 HSE Report", styles["Title"]), Spacer(1, 8)]
        data = [df_states.columns.tolist()] + df_states.astype(str).values.tolist()
        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.lightblue),
            ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
        ]))
        elements.append(table)
        doc.build(elements)
        st.download_button("⬇️ Download PDF", buffer_pdf.getvalue(), "hse_report.pdf")