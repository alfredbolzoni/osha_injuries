# app.py
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import plotly.express as px
import plotly.graph_objects as go
import io
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
import requests

# ==============================
# DB Connection / Query helper
# ==============================
@st.cache_data(show_spinner=False, ttl=600)
def run_query(table: str, select: str = "*", filters: dict = None) -> pd.DataFrame:
    url = f"{st.secrets['supabase']['url']}/rest/v1/{table}"
    headers = {
        "apikey": st.secrets["supabase"]["key"],
        "Authorization": f"Bearer {st.secrets['supabase']['key']}"
    }
    params = {"select": select}

    # Aggiungi filtri dinamici
    if filters:
        for col, val in filters.items():
            params[col] = f"eq.{val}"

    r = requests.get(url, headers=headers, params=params)
    if r.status_code != 200:
        st.error(f"❌ Supabase API error: {r.text}")
        return pd.DataFrame()

    return pd.DataFrame(r.json())

# ==============================
# Page Config
# ==============================
st.set_page_config(page_title="OSHA Workplace Injuries Dashboard", layout="wide")
st.title("📊 OSHA Workplace Injuries Dashboard")

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

    # Yearly KPIs (nationwide)
    df_kpi_years = run_query("""
        SELECT 
            CAST(year AS INT) AS year,
            ROUND(SUM(injuries)::decimal / NULLIF(SUM(hoursworked),0) * 200000, 2) AS trir,
            ROUND(SUM(daysawayfromwork)::decimal / NULLIF(SUM(hoursworked),0) * 200000, 2) AS severity_rate,
            ROUND(SUM(fatalities)::decimal / NULLIF(SUM(employees),0) * 100000, 2) AS fatality_rate
        FROM incidents
        GROUP BY year
        ORDER BY year;
    """)

    if not df_kpi_years.empty:
        latest = df_kpi_years.iloc[-1]
        prev = df_kpi_years.iloc[-2] if len(df_kpi_years) > 1 else None

        trir_delta = f"{latest['trir'] - prev['trir']:+.2f}" if prev is not None else "N/A"
        sev_delta = f"{latest['severity_rate'] - prev['severity_rate']:+.2f}" if prev is not None else "N/A"
        fat_delta = f"{latest['fatality_rate'] - prev['fatality_rate']:+.2f}" if prev is not None else "N/A"

        c1, c2, c3 = st.columns(3)
        c1.metric("💥 TRIR", f"{latest['trir']}", f"Δ vs {prev['year']}: {trir_delta}" if prev is not None else "")
        c2.metric("📆 Severity Rate", f"{latest['severity_rate']}", f"Δ vs {prev['year']}: {sev_delta}" if prev is not None else "")
        c3.metric("☠️ Fatality Rate", f"{latest['fatality_rate']}", f"Δ vs {prev['year']}: {fat_delta}" if prev is not None else "")

        st.info(f"""
        **Indicators (Year {latest['year']}):**
        - **TRIR (Total Recordable Incident Rate)** → Injuries per 200,000 hours worked (~100 FTEs).  
        - **Severity Rate** → Lost workdays per 200,000 hours worked.  
        - **Fatality Rate** → Deaths per 100,000 employees.  
        """)

    # National trend (all years)
    df_trend = run_query("""
        SELECT CAST(year AS INT) AS year, SUM(injuries) AS injuries
        FROM incidents
        GROUP BY year
        ORDER BY year;
    """)
    if not df_trend.empty:
        st.subheader("📈 National Injury Trend")
        fig_trend = px.line(df_trend, x="year", y="injuries", markers=True,
                            labels={"year": "Year", "injuries": "Injuries"})
        fig_trend.update_traces(hovertemplate="Year %{x}<br>Injuries: %{y:,}")
        st.plotly_chart(fig_trend, use_container_width=True)

    # Year selector for map
    years = df_kpi_years["year"].tolist() if not df_kpi_years.empty else []
    selected_year = st.selectbox("📅 Select a Year:", years, index=len(years)-1 if years else 0)

    df_map = run_query("""
        SELECT r.state_name, r.state_code, SUM(i.injuries) AS injuries
        FROM incidents i
        JOIN regions r ON i.state_code = r.state_code
        WHERE CAST(i.year AS INT) = %s
        GROUP BY r.state_name, r.state_code;
    """, [selected_year])

    if not df_map.empty:
        st.subheader(f"🗺️ Geographic Distribution of Injuries ({selected_year})")
        fig_map = px.choropleth(
            df_map,
            locations="state_code", locationmode="USA-states",
            color="injuries", hover_name="state_name",
            hover_data={"state_code": False, "injuries": True},
            scope="usa", color_continuous_scale="Reds",
            labels={"injuries": "Injuries"}
        )
        st.plotly_chart(fig_map, use_container_width=True)

# -------------------------------------------------------------------
# TAB 2 - STATES
# -------------------------------------------------------------------
with tab2:
    st.header("State Analysis")

    states = run_query("""
        SELECT DISTINCT state_name 
        FROM regions 
        WHERE state_name IS NOT NULL 
        ORDER BY state_name;
    """)["state_name"].tolist()
    state_choice = st.selectbox("🗺️ Select a State:", states)

    # KPIs State vs National
    df_state_kpi = run_query("""
        WITH state_data AS (
            SELECT 
                ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS state_trir,
                ROUND(SUM(i.daysawayfromwork)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS state_severity_rate,
                ROUND(SUM(i.fatalities)::decimal / NULLIF(SUM(i.employees),0) * 100000, 2) AS state_fatality_rate
            FROM incidents i
            JOIN regions r ON i.state_code = r.state_code
            WHERE r.state_name = %s
        ),
        national_data AS (
            SELECT 
                ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS nat_trir,
                ROUND(SUM(i.daysawayfromwork)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS nat_severity_rate,
                ROUND(SUM(i.fatalities)::decimal / NULLIF(SUM(i.employees),0) * 100000, 2) AS nat_fatality_rate
            FROM incidents i
        )
        SELECT * FROM state_data, national_data;
    """, [state_choice])

    if not df_state_kpi.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("💥 TRIR", f"{df_state_kpi['state_trir'][0]}", f"National Avg: {df_state_kpi['nat_trir'][0]}")
        c2.metric("📆 Severity", f"{df_state_kpi['state_severity_rate'][0]}", f"National Avg: {df_state_kpi['nat_severity_rate'][0]}")
        c3.metric("☠️ Fatality Rate", f"{df_state_kpi['state_fatality_rate'][0]}", f"National Avg: {df_state_kpi['nat_fatality_rate'][0]}")

    # State trend
    df_state_trend = run_query("""
        SELECT CAST(i.year AS INT) AS year, SUM(i.injuries) AS injuries
        FROM incidents i
        JOIN regions r ON i.state_code = r.state_code
        WHERE r.state_name = %s
        GROUP BY i.year
        ORDER BY year;
    """, [state_choice])

    if not df_state_trend.empty:
        st.subheader(f"📈 Injury Trend in {state_choice}")
        fig_state_trend = px.line(df_state_trend, x="year", y="injuries", markers=True,
                                  labels={"year": "Year", "injuries": "Injuries"})
        fig_state_trend.update_traces(hovertemplate="Year %{x}<br>Injuries: %{y:,}")
        st.plotly_chart(fig_state_trend, use_container_width=True)

    # State summary table
    df_state_table = run_query("""
        SELECT 
            CAST(i.year AS INT) AS "Year",
            SUM(i.injuries) AS "Injuries",
            SUM(i.fatalities) AS "Fatalities",
            SUM(i.daysawayfromwork) AS "Lost Days (DAFW)",
            SUM(i.jobtransferrestriction) AS "Work Restrictions (DJTR)",
            ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS "TRIR (/200k hrs)",
            ROUND(SUM(i.fatalities)::decimal / NULLIF(SUM(i.employees),0) * 100000, 2) AS "Fatality Rate (/100k emp)"
        FROM incidents i
        JOIN regions r ON i.state_code = r.state_code
        WHERE r.state_name = %s
        GROUP BY i.year
        ORDER BY "Year";
    """, [state_choice])

    if not df_state_table.empty:
        st.subheader(f"📋 Summary Data for {state_choice}")
        st.dataframe(
            df_state_table.style.format({
                "Injuries": "{:,}",
                "Fatalities": "{:,}",
                "Lost Days (DAFW)": "{:,}",
                "Work Restrictions (DJTR)": "{:,}",
                "TRIR (/200k hrs)": "{:.2f}",
                "Fatality Rate (/100k emp)": "{:.2f}"
            }),
            use_container_width=True
        )

    st.info("""
    **How to read these indicators:**
    - **Lost Days (DAFW)** → total days lost due to injuries with absence from work.  
      It reflects overall **injury severity**, not only frequency.  
    - **TRIR (/200k hrs)** → injuries per **200,000 hours worked** (~100 FTE-year).  
      Use it to compare states or companies on the **same scale**.
    """)

# -------------------------------------------------------------------
# TAB 3 - SECTORS
# -------------------------------------------------------------------
with tab3:
    st.header("Sector Analysis")

    sectors = run_query("""
        SELECT DISTINCT sector_macro 
        FROM sectors 
        WHERE sector_macro IS NOT NULL 
          AND sector_macro <> ''
        ORDER BY sector_macro;
    """)["sector_macro"].tolist()
    sector_choice = st.selectbox("🏭 Select a Macro Sector:", sectors)

    # KPIs Sector vs National
    df_sector_kpi = run_query("""
        WITH sector_data AS (
            SELECT 
                ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS sector_trir,
                ROUND(SUM(i.daysawayfromwork)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS sector_severity_rate,
                ROUND(SUM(i.fatalities)::decimal / NULLIF(SUM(i.employees),0) * 100000, 2) AS sector_fatality_rate
            FROM incidents i
            JOIN sectors s ON i.naics_code = s.naics_code
            WHERE s.sector_macro = %s
        ),
        national_data AS (
            SELECT 
                ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS nat_trir,
                ROUND(SUM(i.daysawayfromwork)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS nat_severity_rate,
                ROUND(SUM(i.fatalities)::decimal / NULLIF(SUM(i.employees),0) * 100000, 2) AS nat_fatality_rate
            FROM incidents i
        )
        SELECT * FROM sector_data, national_data;
    """, [sector_choice])

    if not df_sector_kpi.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("💥 TRIR", f"{df_sector_kpi['sector_trir'][0]}", f"National Avg: {df_sector_kpi['nat_trir'][0]}")
        c2.metric("📆 Severity", f"{df_sector_kpi['sector_severity_rate'][0]}", f"National Avg: {df_sector_kpi['nat_severity_rate'][0]}")
        c3.metric("☠️ Fatality Rate", f"{df_sector_kpi['sector_fatality_rate'][0]}", f"National Avg: {df_sector_kpi['nat_fatality_rate'][0]}")

    # Top risky sub-sectors (NAICS 3-digit)
    df_subsector = run_query("""
        SELECT LEFT(i.naics_code::text, 3) AS naics3, SUM(i.injuries) AS total_injuries
        FROM incidents i
        JOIN sectors s ON i.naics_code = s.naics_code
        WHERE s.sector_macro = %s
        GROUP BY naics3
        ORDER BY total_injuries DESC
        LIMIT 10;
    """, [sector_choice])
    if not df_subsector.empty:
        st.subheader(f"🏭 Top 10 Risky Sub-sectors in {sector_choice}")
        fig_subsector = px.bar(df_subsector, x="naics3", y="total_injuries",
                               labels={"naics3": "NAICS (3-digit)", "total_injuries": "Injuries"})
        fig_subsector.update_traces(hovertemplate="NAICS %{x}<br>Injuries: %{y:,}")
        st.plotly_chart(fig_subsector, use_container_width=True)

        st.info("""
        **Reading NAICS codes:**
        - **2 digits** identify the macro sector (e.g., `23` = Construction).  
        - **3 digits** identify the sub-sector (e.g., `236` = Building Construction).  
        - Value shown = total injuries recorded in that sub-sector.
        """)

    # Incident rate per macro sector (injuries / 1000 employees)
    df_rate = run_query("""
        SELECT s.sector_macro,
               ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.employees),0) * 1000, 2) AS incident_rate
        FROM incidents i
        JOIN sectors s ON i.naics_code = s.naics_code
        GROUP BY s.sector_macro
        HAVING SUM(i.employees) > 0
        ORDER BY incident_rate DESC;
    """)
    if not df_rate.empty:
        st.subheader("⚖️ Incident Rate by Macro Sector (injuries / 1000 employees)")
        fig_rate = px.bar(df_rate, x="incident_rate", y="sector_macro", orientation="h",
                          labels={"incident_rate": "Injury Rate (/1000 emp)", "sector_macro": "Macro Sector"})
        fig_rate.update_traces(hovertemplate="<b>%{y}</b><br>Rate: %{x}")
        st.plotly_chart(fig_rate, use_container_width=True)

# -------------------------------------------------------------------
# TAB 4 - COMBINED ANALYSIS
# -------------------------------------------------------------------
with tab4:
    st.header("Combined Analysis: State + Sector + Year")

    # Selectors
    years = run_query("""
        SELECT DISTINCT CAST(year AS INT) AS year
        FROM incidents
        WHERE year IS NOT NULL
        ORDER BY year;
    """)["year"].tolist()
    if not years:
        st.error("No years available in database.")
        years = [0]
    year_sel = st.selectbox("📅 Select Year:", years, index=len(years)-1)

    states = run_query("""
        SELECT DISTINCT state_name
        FROM regions
        WHERE state_name IS NOT NULL
        ORDER BY state_name;
    """)["state_name"].tolist()
    default_state = st.session_state.get("selected_state", states[0] if states else "N/A")
    state_index = states.index(default_state) if default_state in states else 0
    state_sel = st.selectbox("🗺️ Select State:", states, index=state_index)

    sectors = run_query("""
        SELECT DISTINCT sector_macro
        FROM sectors
        WHERE sector_macro IS NOT NULL
          AND sector_macro <> ''
        ORDER BY sector_macro;
    """)["sector_macro"].tolist()
    sector_sel = st.selectbox("🏭 Select Macro Sector:", sectors, index=0 if sectors else None)

    # Combined table (clean aliases)
    df_combo = run_query("""
        SELECT 
            CAST(i.year AS INT) AS "Year",
            r.state_name AS "State",
            s.sector_macro AS "Macro Sector",
            SUM(i.injuries) AS "Injuries",
            SUM(i.fatalities) AS "Fatalities",
            ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS "TRIR (/200k hrs)"
        FROM incidents i
        JOIN regions r ON i.state_code = r.state_code
        JOIN sectors s ON i.naics_code = s.naics_code
        WHERE CAST(i.year AS INT) = %s
          AND r.state_name = %s
          AND s.sector_macro = %s
        GROUP BY i.year, r.state_name, s.sector_macro
        ORDER BY "Year";
    """, [year_sel, state_sel, sector_sel])

    if not df_combo.empty:
        st.subheader(f"📊 Data: {state_sel} – {sector_sel} ({year_sel})")
        st.dataframe(
            df_combo.style.format({
                "Injuries": "{:,}",
                "Fatalities": "{:,}",
                "TRIR (/200k hrs)": "{:.2f}"
            }),
            use_container_width=True
        )

        # 1) Multi-year trend for the chosen State + Sector
        df_trend_combo = run_query("""
            SELECT 
                CAST(i.year AS INT) AS "Year",
                SUM(i.injuries) AS "Injuries",
                SUM(i.fatalities) AS "Fatalities"
            FROM incidents i
            JOIN regions r ON i.state_code = r.state_code
            JOIN sectors s ON i.naics_code = s.naics_code
            WHERE r.state_name = %s
              AND s.sector_macro = %s
            GROUP BY i.year
            ORDER BY "Year";
        """, [state_sel, sector_sel])

        if not df_trend_combo.empty:
            st.subheader(f"📈 Injury Trend – {state_sel} ({sector_sel})")
            fig_trend_combo = px.line(
                df_trend_combo,
                x="Year", y="Injuries",
                markers=True,
                labels={"Year": "Year", "Injuries": "Injuries"}
            )
            fig_trend_combo.update_traces(hovertemplate="Year %{x}<br>Injuries: %{y:,}")
            st.plotly_chart(fig_trend_combo, use_container_width=True)

        # 2) KPI Gauge: TRIR vs National (same year)
        df_kpi_combo = run_query("""
            WITH sector_state AS (
                SELECT ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS trir
                FROM incidents i
                JOIN regions r ON i.state_code = r.state_code
                JOIN sectors s ON i.naics_code = s.naics_code
                WHERE CAST(i.year AS INT) = %s
                  AND r.state_name = %s
                  AND s.sector_macro = %s
            ),
            national AS (
                SELECT ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS trir_nat
                FROM incidents i
                WHERE CAST(i.year AS INT) = %s
            )
            SELECT * FROM sector_state, national;
        """, [year_sel, state_sel, sector_sel, year_sel])

        if not df_kpi_combo.empty:
            val = float(df_kpi_combo["trir"][0] or 0)
            ref = float(df_kpi_combo["trir_nat"][0] or 0)

            st.subheader("📌 TRIR vs National Average")
            fig_kpi = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=val,
                delta={"reference": ref, "increasing": {"color": "red"}, "decreasing": {"color": "green"}},
                gauge={
                    "axis": {"range": [0, max(val, ref) * 1.5 if max(val, ref) > 0 else 1]},
                    "bar": {"color": "blue"},
                    "steps": [
                        {"range": [0, ref], "color": "lightgreen"},
                        {"range": [ref, max(val, ref) * 1.5 if max(val, ref) > 0 else 1], "color": "pink"}
                    ],
                },
                title={"text": f"TRIR {state_sel} – {sector_sel} ({year_sel})"}
            ))
            st.plotly_chart(fig_kpi, use_container_width=True)

        st.info("""
        **What you see:**
        - Clean table with **Injuries, Fatalities, TRIR** for the selected filters.
        - Multi-year trend for the chosen State + Macro Sector.
        - TRIR gauge against the national average (same year).
        """)

        # 3) Scenario Simulator (Employees %, Hours %, Injuries %)
        st.subheader("🧪 Scenario Simulator")

        cA, cB, cC = st.columns(3)
        with cA:
            delta_emp = st.slider("Change in Employees (%)", -30, 30, 0, step=5)
        with cB:
            delta_hours = st.slider("Change in Hours Worked (%)", -30, 30, 0, step=5)
        with cC:
            delta_inj = st.slider("Change in Injuries (%)", -50, 50, 0, step=5)

        st.caption("👷 Employees and ⏱️ Hours affect the **denominator** of Fatality Rate and TRIR respectively. Injuries % adjusts the **numerator** for TRIR.")
        
        df_base = run_query("""
            SELECT SUM(i.injuries) AS injuries,
                   SUM(i.fatalities) AS fatalities,
                   SUM(i.employees) AS employees,
                   SUM(i.hoursworked) AS hoursworked
            FROM incidents i
            JOIN regions r ON i.state_code = r.state_code
            JOIN sectors s ON i.naics_code = s.naics_code
            WHERE CAST(i.year AS INT) = %s
              AND r.state_name = %s
              AND s.sector_macro = %s
        """, [year_sel, state_sel, sector_sel])

        if not df_base.empty:
            injuries = float(df_base["injuries"][0] or 0)
            fatalities = float(df_base["fatalities"][0] or 0)
            employees = float(df_base["employees"][0] or 0)
            hours = float(df_base["hoursworked"][0] or 0)

            # Apply variations (guard against zero or negative denominators)
            employees_adj = max(employees * (1 + delta_emp/100), 1.0) if employees else 0.0
            hours_adj     = max(hours * (1 + delta_hours/100), 1.0) if hours else 0.0
            injuries_adj  = max(injuries * (1 + delta_inj/100), 0.0)

            # Original KPIs
            trir_orig = round(injuries / hours * 200000, 2) if hours else 0
            fatality_orig = round(fatalities / employees * 100000, 2) if employees else 0

            # Simulated KPIs
            trir_new = round(injuries_adj / hours_adj * 200000, 2) if hours_adj else 0
            fatality_new = round(fatalities / employees_adj * 100000, 2) if employees_adj else 0

            c1, c2 = st.columns(2)
            with c1:
                st.metric("💥 TRIR (original)", trir_orig)
                st.metric("☠️ Fatality Rate (original)", fatality_orig)
            with c2:
                st.metric("💥 TRIR (simulated)", trir_new, f"{trir_new - trir_orig:+.2f}")
                st.metric("☠️ Fatality Rate (simulated)", fatality_new, f"{fatality_new - fatality_orig:+.2f}")

            st.info(f"""
            **How to interpret the simulator**

            - Sliders adjust **percent changes** to denominators and numerators:  
              • Employees (👷) → denominator of Fatality Rate  
              • Hours worked (⏱️) → denominator of TRIR  
              • Injuries (%) → numerator of TRIR
            - Recorded events (fatalities) remain historical; we do not forecast events.
            - Formulas: TRIR = (Injuries ÷ Hours) × 200,000; Fatality Rate = (Fatalities ÷ Employees) × 100,000.
            - Purpose: quick **what-if analysis** to assess risk metrics under alternative operating conditions.
            """)

    else:
        st.warning("No data for the selected filters.")

# -------------------------------------------------------------------
# TAB 5 - INSIGHTS & EXPORT
# -------------------------------------------------------------------
with tab5:
    st.header("Insights & Reporting")

    # Latest year available for insights
    df_years = run_query("SELECT DISTINCT CAST(year AS INT) AS y FROM incidents ORDER BY y;")
    latest_year = int(df_years["y"].iloc[-1]) if not df_years.empty else None
    st.caption(f"Latest year in dataset: **{latest_year}**" if latest_year else "No year information available.")

    # Top risky states (by TRIR) in latest year
    if latest_year:
        df_state_risk = run_query("""
            SELECT r.state_name AS state,
                   ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS trir
            FROM incidents i
            JOIN regions r ON i.state_code = r.state_code
            WHERE CAST(i.year AS INT) = %s
            GROUP BY r.state_name
            HAVING SUM(i.hoursworked) > 0
            ORDER BY trir DESC
            LIMIT 10;
        """, [latest_year])

        if not df_state_risk.empty:
            st.subheader(f"🔥 Top 10 States by TRIR ({latest_year})")
            fig_top_states = px.bar(df_state_risk, x="trir", y="state", orientation="h",
                                    labels={"trir": "TRIR", "state": "State"})
            fig_top_states.update_traces(hovertemplate="<b>%{y}</b><br>TRIR: %{x}")
            st.plotly_chart(fig_top_states, use_container_width=True)

        # Top risky macro sectors (by TRIR) in latest year
        df_sector_risk = run_query("""
            SELECT s.sector_macro AS sector,
                   ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS trir
            FROM incidents i
            JOIN sectors s ON i.naics_code = s.naics_code
            WHERE CAST(i.year AS INT) = %s
            GROUP BY s.sector_macro
            HAVING SUM(i.hoursworked) > 0
            ORDER BY trir DESC
            LIMIT 10;
        """, [latest_year])

        if not df_sector_risk.empty:
            st.subheader(f"🏭 Top 10 Macro Sectors by TRIR ({latest_year})")
            fig_top_sectors = px.bar(df_sector_risk, x="trir", y="sector", orientation="h",
                                     labels={"trir": "TRIR", "sector": "Macro Sector"})
            fig_top_sectors.update_traces(hovertemplate="<b>%{y}</b><br>TRIR: %{x}")
            st.plotly_chart(fig_top_sectors, use_container_width=True)

    # --------------------------
    # Export helpers
    # --------------------------
    def generate_report(year=None, state=None, sector=None) -> pd.DataFrame:
        query = """
            SELECT 
                CAST(i.year AS INT) AS "Year",
                r.state_name AS "State",
                s.sector_macro AS "Macro Sector",
                SUM(i.injuries) AS "Injuries",
                SUM(i.fatalities) AS "Fatalities",
                ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS "TRIR (/200k hrs)"
            FROM incidents i
            JOIN regions r ON i.state_code = r.state_code
            JOIN sectors s ON i.naics_code = s.naics_code
            WHERE 1=1
        """
        params = []
        if year:
            query += " AND CAST(i.year AS INT) = %s"
            params.append(year)
        if state:
            query += " AND r.state_name = %s"
            params.append(state)
        if sector:
            query += " AND s.sector_macro = %s"
            params.append(sector)

        query += """
            GROUP BY i.year, r.state_name, s.sector_macro
            ORDER BY "Year", "State", "Macro Sector";
        """
        return run_query(query, params)

    st.subheader("📥 Export (Excel / PDF)")
    cexp1, cexp2, cexp3 = st.columns(3)

    with cexp1:
        exp_year = st.selectbox("Year (optional)", [None] + (years if 'years' in locals() and years else []), index=0)
    with cexp2:
        exp_state = st.selectbox("State (optional)", [None] + run_query("""
            SELECT DISTINCT state_name FROM regions WHERE state_name IS NOT NULL ORDER BY state_name;
        """)["state_name"].tolist(), index=0)
    with cexp3:
        exp_sector = st.selectbox("Macro Sector (optional)", [None] + run_query("""
            SELECT DISTINCT sector_macro FROM sectors WHERE sector_macro IS NOT NULL AND sector_macro <> '' ORDER BY sector_macro;
        """)["sector_macro"].tolist(), index=0)

    df_report = generate_report(exp_year, exp_state, exp_sector)

    if not df_report.empty:
        # Excel
        buffer_xlsx = io.BytesIO()
        with pd.ExcelWriter(buffer_xlsx, engine="xlsxwriter") as writer:
            df_report.to_excel(writer, index=False, sheet_name="HSE Report")
        st.download_button(
            label="⬇️ Download Excel",
            data=buffer_xlsx,
            file_name="hse_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # PDF
        buffer_pdf = io.BytesIO()
        doc = SimpleDocTemplate(buffer_pdf, pagesize=A4)
        styles = getSampleStyleSheet()
        elements = []
        elements.append(Paragraph("📊 HSE Report", styles["Title"]))
        elements.append(Spacer(1, 8))

        # optional filters summary
        filt = f"Filters — Year: {exp_year or 'All'} | State: {exp_state or 'All'} | Sector: {exp_sector or 'All'}"
        elements.append(Paragraph(filt, styles["Normal"]))
        elements.append(Spacer(1, 8))

        data = [df_report.columns.tolist()] + df_report.astype(str).values.tolist()
        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.lightblue),
            ("TEXTCOLOR", (0,0), (-1,0), colors.black),
            ("ALIGN", (0,0), (-1,-1), "CENTER"),
            ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
            ("FONTSIZE", (0,0), (-1,-1), 8),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.whitesmoke, colors.lightgrey])
        ]))
        elements.append(table)
        doc.build(elements)

        st.download_button(
            label="⬇️ Download PDF",
            data=buffer_pdf.getvalue(),
            file_name="hse_report.pdf",
            mime="application/pdf"
        )
    else:
        st.warning("No data available for the selected export filters.")