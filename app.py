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

    # 1) Aggregated KPIs directly from Postgres
    df_grp = run_query("""
        SELECT year::int AS year,
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
        # Compute KPIs
        df_grp["TRIR"] = (df_grp["injuries"] / df_grp["hoursworked"]).fillna(0) * 200000
        df_grp["SeverityRate"] = (df_grp["daysawayfromwork"] / df_grp["hoursworked"]).fillna(0) * 200000
        df_grp["FatalityRate"] = (df_grp["fatalities"] / df_grp["employees"]).fillna(0) * 100000

        # Latest vs previous
        latest = df_grp.iloc[-1]
        prev = df_grp.iloc[-2] if len(df_grp) > 1 else None

        def delta_str(metric):
            return f"{latest[metric] - prev[metric]:+.2f}" if prev is not None else "N/A"

        c1, c2, c3 = st.columns(3)
        c1.metric("💥 TRIR", f"{latest['TRIR']:.2f}", f"Δ vs {int(prev['year'])}: {delta_str('TRIR')}" if prev is not None else "")
        c2.metric("📆 Severity Rate", f"{latest['SeverityRate']:.2f}", f"Δ vs {int(prev['year'])}: {delta_str('SeverityRate')}" if prev is not None else "")
        c3.metric("☠️ Fatality Rate", f"{latest['FatalityRate']:.2f}", f"Δ vs {int(prev['year'])}: {delta_str('FatalityRate')}" if prev is not None else "")

        st.info(f"""
        **Indicators (Year {int(latest['year'])}):**
        - **TRIR** → injuries per 200,000 hours worked (~100 FTEs).  
        - **Severity Rate** → lost workdays per 200,000 hours worked.  
        - **Fatality Rate** → deaths per 100,000 employees.  
        """)

        # 2) National trend line
        st.subheader("📈 National Injury Trend")
        fig_trend = px.line(df_grp, x="year", y="injuries", markers=True,
                            labels={"year": "Year", "injuries": "Injuries"})
        fig_trend.update_traces(hovertemplate="Year %{x}<br>Injuries: %{y:,}")
        st.plotly_chart(fig_trend, use_container_width=True)

        # 3) Map by year
        years = df_grp["year"].tolist()
        selected_year = st.selectbox("📅 Select a Year:", years, index=len(years)-1)

        df_map = run_query("""
            SELECT r.state_name, r.state_code, SUM(i.injuries) AS injuries
            FROM incidents i
            JOIN regions r ON i.state_code = r.state_code
            WHERE i.year = %s
            GROUP BY r.state_name, r.state_code
            ORDER BY injuries DESC;
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

    # Load all states
    states = run_query("SELECT DISTINCT state_name FROM regions ORDER BY state_name;")["state_name"].tolist()
    state_choice = st.selectbox("🗺️ Select a State:", states)

    if state_choice:
        # Aggregate only for the selected state
        df_state = run_query("""
            SELECT i.year::int AS year,
                   SUM(i.injuries) AS injuries,
                   SUM(i.fatalities) AS fatalities,
                   SUM(i.hoursworked) AS hoursworked,
                   SUM(i.employees) AS employees,
                   SUM(i.daysawayfromwork) AS daysawayfromwork,
                   SUM(i.jobtransferrestriction) AS jobtransferrestriction
            FROM incidents i
            JOIN regions r ON i.state_code = r.state_code
            WHERE r.state_name = %s
            GROUP BY i.year
            ORDER BY i.year;
        """, [state_choice])

        # National aggregates (for comparison)
        df_nat = run_query("""
            SELECT SUM(injuries) AS injuries,
                   SUM(fatalities) AS fatalities,
                   SUM(hoursworked) AS hoursworked,
                   SUM(employees) AS employees,
                   SUM(daysawayfromwork) AS daysawayfromwork
            FROM incidents;
        """)

        if not df_state.empty and not df_nat.empty:
            st.subheader(f"📊 KPIs – {state_choice} vs National")

            st_sum = df_state.sum(numeric_only=True)
            nat_sum = df_nat.iloc[0]

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

            # 📈 Trend
            st.subheader(f"📈 Injury Trend in {state_choice}")
            fig_state_trend = px.line(
                df_state, x="year", y="injuries", markers=True,
                labels={"year": "Year", "injuries": "Injuries"}
            )
            fig_state_trend.update_traces(hovertemplate="Year %{x}<br>Injuries: %{y:,}")
            st.plotly_chart(fig_state_trend, use_container_width=True)

            # 📋 Summary Table
            df_state["TRIR (/200k hrs)"] = safe_div(df_state["injuries"], df_state["hoursworked"], 200000)
            df_state["Fatality Rate (/100k emp)"] = safe_div(df_state["fatalities"], df_state["employees"], 100000)

            st.subheader(f"📋 Summary for {state_choice}")
            st.dataframe(
                df_state.rename(columns={
                    "year": "Year",
                    "injuries": "Injuries",
                    "fatalities": "Fatalities",
                    "daysawayfromwork": "Lost Days (DAFW)",
                    "jobtransferrestriction": "Work Restrictions (DJTR)"
                }),
                use_container_width=True
            )

            st.info("""
            **How to interpret:**
            - **Lost Days (DAFW)** → total days lost due to injury absences.  
            - **TRIR (/200k hrs)** → injuries per 200,000 hours worked (~100 FTE-year).  
            - **Fatality Rate (/100k emp)** → deaths per 100,000 employees.  
            """)
        else:
            st.warning(f"⚠️ No data available for {state_choice}.")

# -------------------------------------------------------------------
# TAB 3 - SECTORS
# -------------------------------------------------------------------
with tab3:
    st.header("Sector Analysis")

    # Load distinct macro sectors
    macros = run_query("""
        SELECT DISTINCT sector_macro 
        FROM sectors 
        WHERE sector_macro IS NOT NULL AND sector_macro <> ''
        ORDER BY sector_macro;
    """)["sector_macro"].tolist()

    sector_choice = st.selectbox("🏭 Select a Macro Sector:", macros)

    if sector_choice:
        # KPIs for selected sector vs national
        df_sector = run_query("""
            SELECT SUM(i.injuries) AS injuries,
                   SUM(i.fatalities) AS fatalities,
                   SUM(i.hoursworked) AS hoursworked,
                   SUM(i.employees) AS employees,
                   SUM(i.daysawayfromwork) AS daysawayfromwork
            FROM incidents i
            JOIN sectors s ON i.naics_code = s.naics_code
            WHERE s.sector_macro = %s;
        """, [sector_choice])

        df_nat = run_query("""
            SELECT SUM(injuries) AS injuries,
                   SUM(fatalities) AS fatalities,
                   SUM(hoursworked) AS hoursworked,
                   SUM(employees) AS employees,
                   SUM(daysawayfromwork) AS daysawayfromwork
            FROM incidents;
        """)

        if not df_sector.empty and not df_nat.empty:
            sec_sum = df_sector.iloc[0]
            nat_sum = df_nat.iloc[0]

            sec_trir = safe_div(sec_sum["injuries"], sec_sum["hoursworked"], 200000)
            nat_trir = safe_div(nat_sum["injuries"], nat_sum["hoursworked"], 200000)
            sec_sev  = safe_div(sec_sum["daysawayfromwork"], sec_sum["hoursworked"], 200000)
            nat_sev  = safe_div(nat_sum["daysawayfromwork"], nat_sum["hoursworked"], 200000)
            sec_fat  = safe_div(sec_sum["fatalities"], sec_sum["employees"], 100000)
            nat_fat  = safe_div(nat_sum["fatalities"], nat_sum["employees"], 100000)

            c1, c2, c3 = st.columns(3)
            c1.metric("💥 TRIR", sec_trir, f"National: {nat_trir}")
            c2.metric("📆 Severity", sec_sev, f"National: {nat_sev}")
            c3.metric("☠️ Fatality Rate", sec_fat, f"National: {nat_fat}")

        # 🏭 Top risky sub-sectors (NAICS 3-digit)
        df_sub = run_query("""
            SELECT LEFT(i.naics_code::text, 3) AS naics3,
                   SUM(i.injuries) AS total_injuries
            FROM incidents i
            JOIN sectors s ON i.naics_code = s.naics_code
            WHERE s.sector_macro = %s
            GROUP BY naics3
            ORDER BY total_injuries DESC
            LIMIT 10;
        """, [sector_choice])

        if not df_sub.empty:
            st.subheader(f"🏭 Top 10 Risky Sub-sectors in {sector_choice}")
            fig_sub = px.bar(df_sub, x="naics3", y="total_injuries",
                             labels={"naics3": "NAICS (3-digit)", "total_injuries": "Injuries"})
            fig_sub.update_traces(hovertemplate="NAICS %{x}<br>Injuries: %{y:,}")
            st.plotly_chart(fig_sub, use_container_width=True)

            st.info("""
            **Reading NAICS codes:**
            - **2 digits** → macro sector (e.g., `23` = Construction).  
            - **3 digits** → sub-sector (e.g., `236` = Building Construction).  
            - Value = total injuries recorded in that sub-sector.  
            """)

        # ⚖️ Incident rate per macro sector (injuries / 1000 employees)
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
        SELECT DISTINCT year::int AS year
        FROM incidents
        WHERE year IS NOT NULL
        ORDER BY year;
    """)["year"].tolist()

    states = run_query("""
        SELECT DISTINCT state_name
        FROM regions
        WHERE state_name IS NOT NULL
        ORDER BY state_name;
    """)["state_name"].tolist()

    sectors = run_query("""
        SELECT DISTINCT sector_macro
        FROM sectors
        WHERE sector_macro IS NOT NULL AND sector_macro <> ''
        ORDER BY sector_macro;
    """)["sector_macro"].tolist()

    year_sel = st.selectbox("📅 Select Year:", years, index=len(years)-1 if years else 0)
    state_sel = st.selectbox("🗺️ Select State:", states, index=0 if states else None)
    sector_sel = st.selectbox("🏭 Select Macro Sector:", sectors, index=0 if sectors else None)

    # Combined query
    df_combo = run_query("""
        SELECT 
            i.year::int AS year,
            r.state_name AS state,
            s.sector_macro AS sector,
            SUM(i.injuries) AS injuries,
            SUM(i.fatalities) AS fatalities,
            SUM(i.hoursworked) AS hoursworked,
            SUM(i.employees) AS employees
        FROM incidents i
        JOIN regions r ON i.state_code = r.state_code
        JOIN sectors s ON i.naics_code = s.naics_code
        WHERE i.year::int = %s
          AND r.state_name = %s
          AND s.sector_macro = %s
        GROUP BY i.year, r.state_name, s.sector_macro;
    """, [year_sel, state_sel, sector_sel])

    if not df_combo.empty:
        row = df_combo.iloc[0]
        trir = safe_div(row["injuries"], row["hoursworked"], 200000)

        st.subheader(f"📊 {state_sel} – {sector_sel} ({year_sel})")
        st.dataframe(pd.DataFrame({
            "Year": [row["year"]],
            "State": [row["state"]],
            "Sector": [row["sector"]],
            "Injuries": [row["injuries"]],
            "Fatalities": [row["fatalities"]],
            "TRIR (/200k hrs)": [trir],
        }), use_container_width=True)

        # 📈 Trend multi-year for chosen state + sector
        df_trend = run_query("""
            SELECT i.year::int AS year,
                   SUM(i.injuries) AS injuries
            FROM incidents i
            JOIN regions r ON i.state_code = r.state_code
            JOIN sectors s ON i.naics_code = s.naics_code
            WHERE r.state_name = %s
              AND s.sector_macro = %s
            GROUP BY i.year
            ORDER BY year;
        """, [state_sel, sector_sel])

        if not df_trend.empty:
            st.subheader(f"📈 Injury Trend – {state_sel} ({sector_sel})")
            fig_tr = px.line(df_trend, x="year", y="injuries", markers=True,
                             labels={"year": "Year", "injuries": "Injuries"})
            fig_tr.update_traces(hovertemplate="Year %{x}<br>Injuries: %{y:,}")
            st.plotly_chart(fig_tr, use_container_width=True)

        # 📌 KPI gauge vs national
        df_nat = run_query("""
            SELECT 
                SUM(injuries) AS injuries,
                SUM(hoursworked) AS hoursworked
            FROM incidents
            WHERE year::int = %s;
        """, [year_sel])

        if not df_nat.empty:
            val = trir
            ref = safe_div(df_nat.iloc[0]["injuries"], df_nat.iloc[0]["hoursworked"], 200000)

            st.subheader("📌 TRIR vs National Average")
            rng = max(val, ref) * 1.5 if max(val, ref) > 0 else 1
            fig_kpi = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=val,
                delta={"reference": ref, "increasing": {"color": "red"}, "decreasing": {"color": "green"}},
                gauge={
                    "axis": {"range": [0, rng]},
                    "bar": {"color": "blue"},
                    "steps": [
                        {"range": [0, ref], "color": "lightgreen"},
                        {"range": [ref, rng], "color": "pink"}
                    ],
                },
                title={"text": f"TRIR {state_sel} – {sector_sel} ({year_sel})"}
            ))
            st.plotly_chart(fig_kpi, use_container_width=True)

        # 🧪 Scenario Simulator
        st.subheader("🧪 Scenario Simulator")
        cA, cB, cC = st.columns(3)
        with cA:
            delta_emp = st.slider("Change in Employees (%)", -30, 30, 0, step=5)
        with cB:
            delta_hours = st.slider("Change in Hours Worked (%)", -30, 30, 0, step=5)
        with cC:
            delta_inj = st.slider("Change in Injuries (%)", -50, 50, 0, step=5)

        employees = row["employees"]
        hours = row["hoursworked"]
        injuries = row["injuries"]
        fatalities = row["fatalities"]

        emp_adj = max(employees * (1 + delta_emp/100), 1.0) if employees else 0.0
        hrs_adj = max(hours * (1 + delta_hours/100), 1.0) if hours else 0.0
        inj_adj = max(injuries * (1 + delta_inj/100), 0.0)

        trir_orig = safe_div(injuries, hours, 200000)
        fat_orig  = safe_div(fatalities, employees, 100000)
        trir_new  = safe_div(inj_adj, hrs_adj, 200000)
        fat_new   = safe_div(fatalities, emp_adj, 100000)

        d1, d2 = st.columns(2)
        with d1:
            st.metric("💥 TRIR (original)", trir_orig)
            st.metric("☠️ Fatality Rate (original)", fat_orig)
        with d2:
            st.metric("💥 TRIR (simulated)", trir_new, f"{trir_new - trir_orig:+.2f}")
            st.metric("☠️ Fatality Rate (simulated)", fat_new, f"{fat_new - fat_orig:+.2f}")

        st.info("""
        **How to interpret the simulator**
        - Sliders apply percent changes to denominators/numerators:
          • Employees → denominator of Fatality Rate  
          • Hours worked → denominator of TRIR  
          • Injuries (%) → numerator of TRIR
        - Recorded fatalities remain historical; this is a what-if tool, not a predictor.
        """)
    else:
        st.warning("⚠️ No data for selected filters.")

# -------------------------------------------------------------------
# TAB 5 - INSIGHTS & EXPORT
# -------------------------------------------------------------------
with tab5:
    st.header("Insights & Export")

    # Trova l'ultimo anno disponibile
    df_years = run_query("SELECT DISTINCT year::int AS year FROM incidents ORDER BY year;")
    latest_year = df_years["year"].max() if not df_years.empty else None

    if latest_year:
        st.subheader(f"🔥 Insights for {latest_year}")

        # 🔝 Top 10 States by TRIR
        df_states = run_query("""
            SELECT r.state_name,
                   SUM(i.injuries) AS injuries,
                   SUM(i.hoursworked) AS hoursworked,
                   ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS trir
            FROM incidents i
            JOIN regions r ON i.state_code = r.state_code
            WHERE i.year::int = %s
            GROUP BY r.state_name
            HAVING SUM(i.hoursworked) > 0
            ORDER BY trir DESC
            LIMIT 10;
        """, [latest_year])

        if not df_states.empty:
            st.subheader("🔥 Top 10 States by TRIR")
            fig_s = px.bar(df_states, x="trir", y="state_name", orientation="h",
                           labels={"trir": "TRIR", "state_name": "State"})
            fig_s.update_traces(hovertemplate="<b>%{y}</b><br>TRIR: %{x}")
            st.plotly_chart(fig_s, use_container_width=True)

        # 🔝 Top 10 Sectors by TRIR
        df_sectors = run_query("""
            SELECT s.sector_macro,
                   SUM(i.injuries) AS injuries,
                   SUM(i.hoursworked) AS hoursworked,
                   ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS trir
            FROM incidents i
            JOIN sectors s ON i.naics_code = s.naics_code
            WHERE i.year::int = %s
            GROUP BY s.sector_macro
            HAVING SUM(i.hoursworked) > 0
            ORDER BY trir DESC
            LIMIT 10;
        """, [latest_year])

        if not df_sectors.empty:
            st.subheader("🏭 Top 10 Sectors by TRIR")
            fig_m = px.bar(df_sectors, x="trir", y="sector_macro", orientation="h",
                           labels={"trir": "TRIR", "sector_macro": "Macro Sector"})
            fig_m.update_traces(hovertemplate="<b>%{y}</b><br>TRIR: %{x}")
            st.plotly_chart(fig_m, use_container_width=True)

    # --------------------------
    # Export
    # --------------------------
    st.subheader("📥 Export (Excel / PDF)")

    years = df_years["year"].tolist() if not df_years.empty else []
    states = run_query("SELECT DISTINCT state_name FROM regions WHERE state_name IS NOT NULL ORDER BY state_name;")["state_name"].tolist()
    sectors = run_query("SELECT DISTINCT sector_macro FROM sectors WHERE sector_macro IS NOT NULL ORDER BY sector_macro;")["sector_macro"].tolist()

    c1, c2, c3 = st.columns(3)
    with c1:
        exp_year = st.selectbox("Year (optional)", [None] + years, index=0)
    with c2:
        exp_state = st.selectbox("State (optional)", [None] + states, index=0)
    with c3:
        exp_sector = st.selectbox("Macro Sector (optional)", [None] + sectors, index=0)

    # Costruisci la query dinamica
    query = """
        SELECT i.year::int AS year,
               r.state_name AS state,
               s.sector_macro AS sector,
               SUM(i.injuries) AS injuries,
               SUM(i.fatalities) AS fatalities,
               SUM(i.hoursworked) AS hours,
               ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.hoursworked),0) * 200000, 2) AS trir
        FROM incidents i
        JOIN regions r ON i.state_code = r.state_code
        JOIN sectors s ON i.naics_code = s.naics_code
        WHERE 1=1
    """
    params = []
    if exp_year:
        query += " AND i.year::int = %s"
        params.append(exp_year)
    if exp_state:
        query += " AND r.state_name = %s"
        params.append(exp_state)
    if exp_sector:
        query += " AND s.sector_macro = %s"
        params.append(exp_sector)

    query += " GROUP BY i.year, r.state_name, s.sector_macro ORDER BY i.year, r.state_name, s.sector_macro;"

    df_report = run_query(query, params)

    if not df_report.empty:
        # Excel export
        buffer_xlsx = io.BytesIO()
        with pd.ExcelWriter(buffer_xlsx, engine="xlsxwriter") as writer:
            df_report.to_excel(writer, index=False, sheet_name="HSE Report")
        st.download_button("⬇️ Download Excel", buffer_xlsx.getvalue(), "hse_report.xlsx")

        # PDF export
        buffer_pdf = io.BytesIO()
        doc = SimpleDocTemplate(buffer_pdf, pagesize=A4)
        styles = getSampleStyleSheet()
        elements = [Paragraph("📊 HSE Report", styles["Title"]), Spacer(1, 8)]
        filt = f"Filters — Year: {exp_year or 'All'} | State: {exp_state or 'All'} | Sector: {exp_sector or 'All'}"
        elements.append(Paragraph(filt, styles["Normal"]))
        elements.append(Spacer(1, 8))

        data = [df_report.columns.tolist()] + df_report.astype(str).values.tolist()
        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.lightblue),
            ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
            ("ALIGN", (0,0), (-1,-1), "CENTER"),
        ]))
        elements.append(table)
        doc.build(elements)

        st.download_button("⬇️ Download PDF", buffer_pdf.getvalue(), "hse_report.pdf")
    else:
        st.warning("⚠️ No data available for selected export filters.")