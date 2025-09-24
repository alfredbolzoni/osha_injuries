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

@st.cache_data(show_spinner=True, ttl=3600)
def load_incidents():
    """
    Load incidents aggregated by year + state_code + naics_code.
    This reduces the dataset size massively while keeping all metrics usable.
    """
    return run_query("""
        SELECT year, state_code, naics_code,
               SUM(injuries) AS injuries,
               SUM(fatalities) AS fatalities,
               SUM(hoursworked) AS hoursworked,
               SUM(employees) AS employees,
               SUM(daysawayfromwork) AS daysawayfromwork,
               SUM(jobtransferrestriction) AS jobtransferrestriction
        FROM incidents
        GROUP BY year, state_code, naics_code
        ORDER BY year, state_code, naics_code;
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
    st.markdown(
        "<h2 style='margin-bottom:0'>National Overview</h2>",
        unsafe_allow_html=True
    )

    # 1) Aggregated KPIs
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
        # KPIs
        df_grp["TRIR"] = (df_grp["injuries"] / df_grp["hoursworked"]).fillna(0) * 200000
        df_grp["SeverityRate"] = (df_grp["daysawayfromwork"] / df_grp["hoursworked"]).fillna(0) * 200000
        df_grp["FatalityRate"] = (df_grp["fatalities"] / df_grp["employees"]).fillna(0) * 100000

        latest = df_grp.iloc[-1]
        prev = df_grp.iloc[-2] if len(df_grp) > 1 else None

        def delta_str(metric):
            return f"{latest[metric] - prev[metric]:+.2f}" if prev is not None else "N/A"

        c1, c2, c3 = st.columns(3)
        c1.metric("💥 TRIR", f"{latest['TRIR']:.2f}", f"Δ {int(prev['year'])}: {delta_str('TRIR')}" if prev is not None else "")
        c2.metric("📆 Severity Rate", f"{latest['SeverityRate']:.2f}", f"Δ {int(prev['year'])}: {delta_str('SeverityRate')}" if prev is not None else "")
        c3.metric("☠️ Fatality Rate", f"{latest['FatalityRate']:.2f}", f"Δ {int(prev['year'])}: {delta_str('FatalityRate')}" if prev is not None else "")

        # Compact info box
        st.markdown("""
        <div style='background:#f0f2f6;padding:12px;border-radius:8px;margin-top:10px'>
        <b>📌 Indicator definitions</b><br>
        • <b>TRIR</b> = Injuries ÷ Hours × 200,000<br>
        • <b>Severity</b> = Days lost ÷ Hours × 200,000<br>
        • <b>Fatality Rate</b> = Fatalities ÷ Employees × 100,000
        </div>
        """, unsafe_allow_html=True)

        # 2) National injury trend
        st.subheader("📈 National Injury Trend")
        fig_trend = px.line(
            df_grp, x="year", y="injuries", markers=True,
            labels={"year": "Year", "injuries": "Injuries"}
        )
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
                scope="usa", color_continuous_scale="Blues",
                labels={"injuries": "Injuries"}
            )
            st.plotly_chart(fig_map, use_container_width=True)

# -------------------------------------------------------------------
# TAB 2 - STATES
# -------------------------------------------------------------------
with tab2:
    st.markdown(
        "<h2 style='margin-bottom:0'>State Analysis</h2>",
        unsafe_allow_html=True
    )

    # Elenco stati
    states = sorted(df_regions["state_name"].dropna().unique().tolist())
    state_choice = st.selectbox("🗺️ Select a State:", states)

    # Filtro dataset
    df_state = incidents_with_state()
    df_state = df_state[df_state["state_name"] == state_choice]

    if not df_state.empty:
        # Aggregati nazionale e stato
        nat_sum = incidents_with_state().sum(numeric_only=True)
        st_sum = df_state.sum(numeric_only=True)

        # KPI
        state_trir = safe_div(st_sum.get("injuries", 0), st_sum.get("hoursworked", 0), 200000)
        nat_trir   = safe_div(nat_sum.get("injuries", 0), nat_sum.get("hoursworked", 0), 200000)

        state_sev  = safe_div(st_sum.get("daysawayfromwork", 0), st_sum.get("hoursworked", 0), 200000)
        nat_sev    = safe_div(nat_sum.get("daysawayfromwork", 0), nat_sum.get("hoursworked", 0), 200000)

        state_fat  = safe_div(st_sum.get("fatalities", 0), st_sum.get("employees", 0), 100000)
        nat_fat    = safe_div(nat_sum.get("fatalities", 0), nat_sum.get("employees", 0), 100000)

        # KPI widgets
        c1, c2, c3 = st.columns(3)
        c1.metric("💥 TRIR", f"{state_trir:.2f}", f"National: {nat_trir:.2f}")
        c2.metric("📆 Severity", f"{state_sev:.2f}", f"National: {nat_sev:.2f}")
        c3.metric("☠️ Fatality Rate", f"{state_fat:.2f}", f"National: {nat_fat:.2f}")

        # Trend multi-anno
        df_trend = df_state.groupby("year")["injuries"].sum().reset_index()
        if not df_trend.empty:
            st.subheader(f"📈 Injury Trend in {state_choice}")
            fig_state_trend = px.line(
                df_trend, x="year", y="injuries", markers=True,
                labels={"year": "Year", "injuries": "Injuries"}
            )
            fig_state_trend.update_traces(hovertemplate="Year %{x}<br>Injuries: %{y:,}")
            st.plotly_chart(fig_state_trend, use_container_width=True)

        # Tabella riassuntiva
        df_table = df_state.groupby("year").agg({
            "injuries": "sum",
            "fatalities": "sum",
            "daysawayfromwork": "sum",
            "jobtransferrestriction": "sum",
            "hoursworked": "sum",
            "employees": "sum"
        }).reset_index()

        df_table["TRIR (/200k hrs)"] = (
            df_table["injuries"] / df_table["hoursworked"]
        ).fillna(0) * 200000
        df_table["Fatality Rate (/100k emp)"] = (
            df_table["fatalities"] / df_table["employees"]
        ).fillna(0) * 100000

        if not df_table.empty:
            st.subheader(f"📋 Summary for {state_choice}")
            st.dataframe(
                df_table.rename(columns={
                    "year": "Year",
                    "injuries": "Injuries",
                    "fatalities": "Fatalities",
                    "daysawayfromwork": "Lost Days (DAFW)",
                    "jobtransferrestriction": "Work Restrictions (DJTR)"
                }),
                use_container_width=True
            )

        # Compact info box
        st.markdown("""
        <div style='background:#f0f2f6;padding:12px;border-radius:8px;margin-top:10px'>
        <b>📌 How to read indicators</b><br>
        • <b>Lost Days (DAFW)</b> = total days lost due to absence from work<br>
        • <b>TRIR (/200k hrs)</b> = injuries per 200,000 hours worked<br>
        • <b>Fatality Rate (/100k emp)</b> = deaths per 100,000 employees
        </div>
        """, unsafe_allow_html=True)

# -------------------------------------------------------------------
# TAB 3 - SECTORS
# -------------------------------------------------------------------
with tab3:
    st.markdown(
        "<h2 style='margin-bottom:0'>Sector Analysis</h2>",
        unsafe_allow_html=True
    )

    # Elenco macro settori
    macros = sorted(df_sectors["sector_macro"].dropna().unique().tolist())
    sector_choice = st.selectbox("🏭 Select a Macro Sector:", macros)

    df_sec = incidents_with_state_sector()
    df_sec = df_sec[df_sec["sector_macro"] == sector_choice]

    if not df_sec.empty:
        nat_sum = incidents_with_state_sector().sum(numeric_only=True)
        sec_sum = df_sec.sum(numeric_only=True)

        # KPI
        sec_trir = safe_div(sec_sum.get("injuries", 0), sec_sum.get("hoursworked", 0), 200000)
        nat_trir = safe_div(nat_sum.get("injuries", 0), nat_sum.get("hoursworked", 0), 200000)

        sec_sev  = safe_div(sec_sum.get("daysawayfromwork", 0), sec_sum.get("hoursworked", 0), 200000)
        nat_sev  = safe_div(nat_sum.get("daysawayfromwork", 0), nat_sum.get("hoursworked", 0), 200000)

        sec_fat  = safe_div(sec_sum.get("fatalities", 0), sec_sum.get("employees", 0), 100000)
        nat_fat  = safe_div(nat_sum.get("fatalities", 0), nat_sum.get("employees", 0), 100000)

        # KPI widgets
        c1, c2, c3 = st.columns(3)
        c1.metric("💥 TRIR", f"{sec_trir:.2f}", f"National: {nat_trir:.2f}")
        c2.metric("📆 Severity", f"{sec_sev:.2f}", f"National: {nat_sev:.2f}")
        c3.metric("☠️ Fatality Rate", f"{sec_fat:.2f}", f"National: {nat_fat:.2f}")

        # Top risky sub-sectors (NAICS 3-digit)
        df_sec["naics3"] = df_sec["naics_code"].astype(str).str[:3]
        top_sub = (
            df_sec.groupby("naics3")["injuries"]
            .sum()
            .reset_index()
            .sort_values("injuries", ascending=False)
            .head(10)
        )

        if not top_sub.empty:
            st.subheader(f"🏭 Top 10 Risky Sub-sectors in {sector_choice}")
            fig_sub = px.bar(
                top_sub, x="naics3", y="injuries",
                labels={"naics3": "NAICS (3-digit)", "injuries": "Injuries"}
            )
            fig_sub.update_traces(hovertemplate="NAICS %{x}<br>Injuries: %{y:,}")
            st.plotly_chart(fig_sub, use_container_width=True)

        # Compact info box
        st.markdown("""
        <div style='background:#f0f2f6;padding:12px;border-radius:8px;margin-top:10px'>
        <b>📌 Reading NAICS codes</b><br>
        • <b>2 digits</b> = macro sector (e.g. 23 = Construction)<br>
        • <b>3 digits</b> = sub-sector (e.g. 236 = Building Construction)<br>
        • Value shown = total injuries recorded
        </div>
        """, unsafe_allow_html=True)

        # Incident rate per macro sector
        df_rate = incidents_with_state_sector()
        if not df_rate.empty:
            rate = (
                df_rate.groupby("sector_macro", dropna=True)
                .agg({"injuries": "sum", "employees": "sum"})
                .reset_index()
            )
            rate = rate[rate["employees"] > 0]
            rate["Incident Rate (/1000 emp)"] = (
                rate["injuries"] / rate["employees"]
            ).fillna(0) * 1000
            rate = rate.sort_values("Incident Rate (/1000 emp)", ascending=False)

            if not rate.empty:
                st.subheader("⚖️ Injury Rate by Macro Sector (injuries / 1000 employees)")
                fig_rate = px.bar(
                    rate, x="Incident Rate (/1000 emp)", y="sector_macro", orientation="h",
                    labels={"sector_macro": "Macro Sector"}
                )
                fig_rate.update_traces(hovertemplate="<b>%{y}</b><br>Rate: %{x}")
                st.plotly_chart(fig_rate, use_container_width=True)

# -------------------------------------------------------------------
# TAB 4 - COMBINED ANALYSIS
# -------------------------------------------------------------------
with tab4:
    st.markdown(
        "<h2 style='margin-bottom:0'>Combined Analysis: State × Sector × Year</h2>",
        unsafe_allow_html=True
    )

    df_all = incidents_with_state_sector()
    if not df_all.empty:
        # Selettori
        years = sorted(df_all["year"].dropna().unique().astype(int).tolist())
        states = sorted(df_all["state_name"].dropna().unique().tolist())
        sectors = sorted(df_all["sector_macro"].dropna().unique().tolist())

        with st.form("filters_combined"):
            c1, c2, c3 = st.columns(3)
            with c1:
                year_sel = st.selectbox("📅 Year", years, index=len(years)-1)
            with c2:
                state_sel = st.selectbox("🗺️ State", states)
            with c3:
                sector_sel = st.selectbox("🏭 Macro Sector", sectors)
            submitted = st.form_submit_button("Apply Filters")

        df_f = df_all[
            (df_all["year"] == year_sel) &
            (df_all["state_name"] == state_sel) &
            (df_all["sector_macro"] == sector_sel)
        ]

        if not df_f.empty:
            # Tabella KPI base
            tbl = df_f.agg({
                "injuries": "sum",
                "fatalities": "sum",
                "hoursworked": "sum",
                "employees": "sum"
            }).to_frame().T
            tbl["TRIR (/200k hrs)"] = safe_div(
                tbl["injuries"].get(0, 0), tbl["hoursworked"].get(0, 0), 200000
            )

            st.subheader(f"📊 {state_sel} – {sector_sel} ({year_sel})")
            st.dataframe(tbl, use_container_width=True)

            # Multi-year trend
            df_tr = (
                df_all[(df_all["state_name"] == state_sel) &
                       (df_all["sector_macro"] == sector_sel)]
                .groupby("year")["injuries"].sum().reset_index()
            )
            if not df_tr.empty:
                st.subheader(f"📈 Injury Trend – {state_sel} ({sector_sel})")
                fig_tr = px.line(
                    df_tr, x="year", y="injuries", markers=True,
                    labels={"year": "Year", "injuries": "Injuries"}
                )
                fig_tr.update_traces(hovertemplate="Year %{x}<br>Injuries: %{y:,}")
                st.plotly_chart(fig_tr, use_container_width=True)

            # Gauges TRIR + Fatality Rate
            nat = df_all[df_all["year"] == year_sel]
            val_trir = safe_div(tbl["injuries"].get(0, 0), tbl["hoursworked"].get(0, 0), 200000)
            ref_trir = safe_div(nat["injuries"].sum(), nat["hoursworked"].sum(), 200000)

            val_fat = safe_div(tbl["fatalities"].get(0, 0), tbl["employees"].get(0, 0), 100000)
            ref_fat = safe_div(nat["fatalities"].sum(), nat["employees"].sum(), 100000)

            c1, c2 = st.columns(2)
            with c1:
                st.subheader("📌 TRIR vs National Average")
                rng = max(val_trir, ref_trir) * 1.5 if max(val_trir, ref_trir) > 0 else 1
                fig_trir = go.Figure(go.Indicator(
                    mode="gauge+number+delta",
                    value=val_trir,
                    delta={"reference": ref_trir,
                           "increasing": {"color": "red"},
                           "decreasing": {"color": "green"}},
                    gauge={"axis": {"range": [0, rng]}, "bar": {"color": "blue"}},
                    number={"font": {"size": 36, "color": "black"}},
                    title={"text": f"TRIR {state_sel} – {sector_sel} ({year_sel})",
                           "font": {"size": 14}}
                ))
                st.plotly_chart(fig_trir, use_container_width=True)

            with c2:
                st.subheader("📌 Fatality Rate vs National Average")
                rng = max(val_fat, ref_fat) * 1.5 if max(val_fat, ref_fat) > 0 else 1
                fig_fat = go.Figure(go.Indicator(
                    mode="gauge+number+delta",
                    value=val_fat,
                    delta={"reference": ref_fat,
                           "increasing": {"color": "red"},
                           "decreasing": {"color": "green"}},
                    gauge={"axis": {"range": [0, rng]}, "bar": {"color": "blue"}},
                    number={"font": {"size": 36, "color": "black"}},
                    title={"text": f"Fatality Rate {state_sel} – {sector_sel} ({year_sel})",
                           "font": {"size": 14}}
                ))
                st.plotly_chart(fig_fat, use_container_width=True)

            # Scenario Simulator
            st.subheader("🧪 Scenario Simulator")
            cA, cB, cC = st.columns(3)
            with cA:
                delta_emp = st.slider("Employees %", -30, 30, 0, step=5)
            with cB:
                delta_hours = st.slider("Hours Worked %", -30, 30, 0, step=5)
            with cC:
                delta_inj = st.slider("Injuries %", -50, 50, 0, step=5)

            base = df_f.agg({
                "injuries": "sum", "fatalities": "sum",
                "employees": "sum", "hoursworked": "sum"
            })
            injuries = float(base.get("injuries", 0) or 0)
            fatalities = float(base.get("fatalities", 0) or 0)
            employees = float(base.get("employees", 0) or 0)
            hours = float(base.get("hoursworked", 0) or 0)

            employees_adj = max(employees * (1 + delta_emp/100), 1.0) if employees else 0.0
            hours_adj     = max(hours * (1 + delta_hours/100), 1.0) if hours else 0.0
            injuries_adj  = max(injuries * (1 + delta_inj/100), 0.0)

            trir_orig = safe_div(injuries, hours, 200000)
            fatality_orig = safe_div(fatalities, employees, 100000)
            trir_new = safe_div(injuries_adj, hours_adj, 200000)
            fatality_new = safe_div(fatalities, employees_adj, 100000)

            c1, c2 = st.columns(2)
            with c1:
                st.metric("💥 TRIR (original)", f"{trir_orig:.2f}")
                st.metric("☠️ Fatality Rate (original)", f"{fatality_orig:.2f}")
            with c2:
                st.metric("💥 TRIR (simulated)", f"{trir_new:.2f}", f"{trir_new - trir_orig:+.2f}")
                st.metric("☠️ Fatality Rate (simulated)", f"{fatality_new:.2f}", f"{fatality_new - fatality_orig:+.2f}")

            # Compact info box
            st.markdown("""
            <div style='background:#f0f2f6;padding:12px;border-radius:8px;margin-top:10px'>
            <b>📌 How to interpret the simulator</b><br>
            • Employees (👷) → denominator of Fatality Rate<br>
            • Hours worked (⏱️) → denominator of TRIR<br>
            • Injuries (%) → numerator of TRIR<br>
            • Fatalities are kept historical (no forecasting)<br>
            </div>
            """, unsafe_allow_html=True)

        else:
            st.warning("⚠️ No data for the selected filters.")
    else:
        st.error("⚠️ Combined dataset is empty.")

# -------------------------------------------------------------------
# TAB 5 - INSIGHTS & EXPORT
# -------------------------------------------------------------------
with tab5:
    st.markdown(
        "<h2 style='margin-bottom:0'>Insights & Export</h2>",
        unsafe_allow_html=True
    )

    df_all = incidents_with_state_sector()
    if not df_all.empty:
        latest_year = df_all["year"].max()

        # 🔝 Top States by TRIR
        df_states = (
            df_all[df_all["year"] == latest_year]
            .groupby("state_name")
            .agg(injuries=("injuries", "sum"), hours=("hoursworked", "sum"))
            .reset_index()
        )
        df_states["TRIR (/200k hrs)"] = df_states.apply(
            lambda r: safe_div(r.get("injuries", 0), r.get("hours", 0), 200000), axis=1
        )
        df_states = df_states.sort_values("TRIR (/200k hrs)", ascending=False).head(10)

        # 🔧 Rename for readability
        df_states = df_states.rename(columns={
            "state_name": "State",
            "injuries": "Injuries",
            "hours": "Hours Worked"
        })

        st.subheader(f"🔥 Top 10 States by TRIR ({latest_year})")
        fig_s = px.bar(
            df_states, x="TRIR (/200k hrs)", y="State", orientation="h",
            labels={"State": "State", "TRIR (/200k hrs)": "TRIR (/200k hrs)"}
        )
        fig_s.update_traces(hovertemplate="<b>%{y}</b><br>TRIR: %{x:.2f}")
        st.plotly_chart(fig_s, use_container_width=True)

        # 🔝 Top Sectors by TRIR
        df_secs = (
            df_all[df_all["year"] == latest_year]
            .groupby("sector_macro")
            .agg(injuries=("injuries", "sum"), hours=("hoursworked", "sum"))
            .reset_index()
        )
        df_secs["TRIR (/200k hrs)"] = df_secs.apply(
            lambda r: safe_div(r.get("injuries", 0), r.get("hours", 0), 200000), axis=1
        )
        df_secs = df_secs.sort_values("TRIR (/200k hrs)", ascending=False).head(10)

        # 🔧 Rename for readability
        df_secs = df_secs.rename(columns={
            "sector_macro": "Sector Macro",
            "injuries": "Injuries",
            "hours": "Hours Worked"
        })

        st.subheader(f"🏭 Top 10 Sectors by TRIR ({latest_year})")
        fig_m = px.bar(
            df_secs, x="TRIR (/200k hrs)", y="Sector Macro", orientation="h",
            labels={"Sector Macro": "Sector Macro", "TRIR (/200k hrs)": "TRIR (/200k hrs)"}
        )
        fig_m.update_traces(hovertemplate="<b>%{y}</b><br>TRIR: %{x:.2f}")
        st.plotly_chart(fig_m, use_container_width=True)

        # 📤 Export section
        st.subheader("⬇️ Export Reports")

        c1, c2 = st.columns(2)

        # Excel
        with c1:
            buffer_xlsx = io.BytesIO()
            with pd.ExcelWriter(buffer_xlsx, engine="xlsxwriter") as writer:
                df_all.to_excel(writer, index=False, sheet_name="Incidents")
            st.download_button(
                "📑 Download Excel", buffer_xlsx.getvalue(), "hse_report.xlsx"
            )

        # PDF
        with c2:
            buffer_pdf = io.BytesIO()
            doc = SimpleDocTemplate(buffer_pdf, pagesize=A4)
            styles = getSampleStyleSheet()
            elements = [
                Paragraph("📊 HSE Report", styles["Title"]),
                Spacer(1, 12),
                Paragraph(f"Top 10 States by TRIR ({latest_year})", styles["Heading2"]),
                Spacer(1, 6)
            ]
            data = [df_states.columns.tolist()] + df_states.astype(str).values.tolist()
            table = Table(data, repeatRows=1)
            table.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.lightblue),
                ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
            ]))
            elements.append(table)
            doc.build(elements)

            st.download_button(
                "📄 Download PDF", buffer_pdf.getvalue(), "hse_report.pdf"
            )

        # Compact info box
        st.markdown("""
        <div style='background:#f0f2f6;padding:12px;border-radius:8px;margin-top:10px'>
        <b>📌 Export Guide</b><br>
        • <b>Excel</b>: full incident dataset, pivot-ready<br>
        • <b>PDF</b>: summary table with top states<br>
        </div>
        """, unsafe_allow_html=True)

    else:
        st.error("⚠️ No data available for Insights & Export.")