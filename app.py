# app.py — OSHA Workplace Injuries Dashboard (Supabase REST edition, full file)

import io
import requests
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

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
# Supabase REST helpers
# ==============================
def _sb_headers() -> dict:
    return {
        "apikey": st.secrets["supabase"]["key"],
        "Authorization": f"Bearer {st.secrets['supabase']['key']}",
    }

@st.cache_data(show_spinner=False, ttl=600)
def sb_select_all(
    table: str,
    select: str = "*",
    filters: dict | None = None,
    page_size: int = 20000,
    max_pages: int = 2000,
) -> pd.DataFrame:
    """
    Fetch ALL rows from a Supabase table via PostgREST pagination.
    Uses the HTTP `Range` header to page through results until exhausted.
    """
    base = st.secrets["supabase"]["url"].rstrip("/")
    url = f"{base}/rest/v1/{table}"
    params = {"select": select}
    if filters:
        for col, val in filters.items():
            params[col] = f"eq.{val}"

    frames = []
    start = 0
    for _ in range(max_pages):
        end = start + page_size - 1
        headers = _sb_headers() | {"Range": f"{start}-{end}"}
        r = requests.get(url, headers=headers, params=params, timeout=90)
        if r.status_code not in (200, 206):
            # 206 = Partial Content when more pages exist
            break
        chunk = pd.DataFrame(r.json())
        if chunk.empty:
            break
        frames.append(chunk)
        if len(chunk) < page_size:
            break
        start += page_size

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)

# ==============================
# Data access layer
# ==============================
@st.cache_data(show_spinner=False, ttl=600)
def load_regions() -> pd.DataFrame:
    # Expect: state_code, state_name
    df = sb_select_all("regions", select="state_code,state_name")
    return df.drop_duplicates()

@st.cache_data(show_spinner=False, ttl=600)
def load_sectors() -> pd.DataFrame:
    # Expect: naics_code, sector_macro
    df = sb_select_all("sectors", select="naics_code,sector_macro").drop_duplicates()
    if "sector_macro" in df.columns:
        df["sector_macro"] = df["sector_macro"].fillna("").str.strip()
        df = df[df["sector_macro"] != ""]
    return df

@st.cache_data(show_spinner=False, ttl=600)
def load_incidents() -> pd.DataFrame:
    # Expect: year,state_code,naics_code,injuries,fatalities,hoursworked,employees,daysawayfromwork,jobtransferrestriction
    cols = "year,state_code,naics_code,injuries,fatalities,hoursworked,employees,daysawayfromwork,jobtransferrestriction"
    df = sb_select_all("incidents", select=cols)

    # Fix year to int (nullable)
    if "year" in df.columns:
        df["year"] = (
            df["year"]
            .astype(str)
            .str.strip()
            .str.extract(r"(\d{4})")[0]
            .astype("Int64")
        )

    # Coerce numerics
    for c in [
        "injuries",
        "fatalities",
        "hoursworked",
        "employees",
        "daysawayfromwork",
        "jobtransferrestriction",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

# Load base tables
df_regions = load_regions()
df_sectors = load_sectors()
df_inc = load_incidents()

st.subheader("🔎 Debug merge inputs")

st.write("Incidents sample:", df_inc.head())
st.write("Regions sample:", df_regions.head())
st.write("Sectors sample:", df_sectors.head())

st.write("Unique state_code in incidents:", df_inc["state_code"].dropna().unique()[:20])
st.write("Unique state_code in regions:", df_regions["state_code"].dropna().unique()[:20])

st.write("Unique naics_code in incidents:", df_inc["naics_code"].dropna().unique()[:20])
st.write("Unique naics_code in sectors:", df_sectors["naics_code"].dropna().unique()[:20])

# ==============================
# Merge helpers
# ==============================
def incidents_with_state() -> pd.DataFrame:
    if df_inc.empty or df_regions.empty:
        return pd.DataFrame()
    return df_inc.merge(df_regions, on="state_code", how="left")

def incidents_with_state_sector() -> pd.DataFrame:
    df1 = incidents_with_state()
    if df1.empty or df_sectors.empty:
        return pd.DataFrame()
    return df1.merge(df_sectors, on="naics_code", how="left")

# ==============================
# Utility
# ==============================
def safe_div(num, den, factor=1.0, ndigits=2) -> float:
    if pd.isna(num) or pd.isna(den) or den == 0:
        return 0.0
    return round((num / den) * factor, ndigits)

# ==============================
# Tabs
# ==============================
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["📊 Overview", "🗺️ States", "🏭 Sectors", "🔍 Combined Analysis", "💡 Insights & Export"]
)

# -------------------------------------------------------------------
# TAB 1 - OVERVIEW
# -------------------------------------------------------------------
with tab1:
    st.header("National Overview")

    if df_inc.empty:
        st.error("⚠️ No data available in 'incidents'.")
    else:
        grp = (
            df_inc.groupby("year", dropna=True)
            .agg(
                injuries=("injuries", "sum"),
                fatalities=("fatalities", "sum"),
                hoursworked=("hoursworked", "sum"),
                employees=("employees", "sum"),
                daysawayfromwork=("daysawayfromwork", "sum"),
            )
            .reset_index()
            .sort_values("year")
        )

        # KPI calculations
        grp["TRIR"] = (grp["injuries"] / grp["hoursworked"]).fillna(0) * 200000
        grp["SeverityRate"] = (grp["daysawayfromwork"] / grp["hoursworked"]).fillna(0) * 200000
        grp["FatalityRate"] = (grp["fatalities"] / grp["employees"]).fillna(0) * 100000

        if not grp.empty:
            latest = grp.iloc[-1]
            prev = grp.iloc[-2] if len(grp) > 1 else None

            def delta_str(metric: str) -> str:
                if prev is None:
                    return "N/A"
                return f"{latest[metric] - prev[metric]:+.2f}"

            c1, c2, c3 = st.columns(3)
            c1.metric(
                "💥 TRIR",
                f"{latest['TRIR']:.2f}",
                f"Δ vs {int(prev['year'])}: {delta_str('TRIR')}" if prev is not None else "",
            )
            c2.metric(
                "📆 Severity Rate",
                f"{latest['SeverityRate']:.2f}",
                f"Δ vs {int(prev['year'])}: {delta_str('SeverityRate')}" if prev is not None else "",
            )
            c3.metric(
                "☠️ Fatality Rate",
                f"{latest['FatalityRate']:.2f}",
                f"Δ vs {int(prev['year'])}: {delta_str('FatalityRate')}" if prev is not None else "",
            )

            st.info(
                f"""
**Indicators (Year {int(latest['year'])}):**
- **TRIR** → injuries per 200,000 hours worked (~100 FTEs).
- **Severity Rate** → lost workdays per 200,000 hours worked.
- **Fatality Rate** → deaths per 100,000 employees.
"""
            )

        # National trend (injuries)
        st.subheader("📈 National Injury Trend")
        fig_trend = px.line(
            grp,
            x="year",
            y="injuries",
            markers=True,
            labels={"year": "Year", "injuries": "Injuries"},
        )
        fig_trend.update_traces(hovertemplate="Year %{x}<br>Injuries: %{y:,}")
        st.plotly_chart(fig_trend, use_container_width=True)

        # Year selector for map (from grouped years to ensure all are present)
        years = grp["year"].dropna().astype(int).tolist()
        selected_year = st.selectbox("📅 Select a Year:", years, index=len(years) - 1)

        # Map by selected year
        df_y = incidents_with_state()
        if not df_y.empty:
            df_map = (
                df_y[df_y["year"] == selected_year]
                .groupby(["state_code", "state_name"], dropna=True)["injuries"]
                .sum()
                .reset_index()
            )
            if not df_map.empty:
                st.subheader(f"🗺️ Geographic Distribution of Injuries ({selected_year})")
                fig_map = px.choropleth(
                    df_map,
                    locations="state_code",
                    locationmode="USA-states",
                    color="injuries",
                    hover_name="state_name",
                    hover_data={"state_code": False, "injuries": True},
                    scope="usa",
                    color_continuous_scale="Reds",
                    labels={"injuries": "Injuries"},
                )
                st.plotly_chart(fig_map, use_container_width=True)

# -------------------------------------------------------------------
# TAB 2 - STATES
# -------------------------------------------------------------------
with tab2:
    st.header("State Analysis")

    if df_regions.empty:
        st.error("⚠️ No states found in 'regions'.")
    else:
        states = sorted(df_regions["state_name"].dropna().unique().tolist())
        state_choice = st.selectbox("🗺️ Select a State:", states)

        df_ss = incidents_with_state()
        df_state = df_ss[df_ss["state_name"] == state_choice]

        # KPIs State vs National (aggregated over all available years)
        nat_sum = df_ss.sum(numeric_only=True)
        st_sum = df_state.sum(numeric_only=True)

        state_trir = safe_div(st_sum.get("injuries", 0), st_sum.get("hoursworked", 0), 200000)
        nat_trir = safe_div(nat_sum.get("injuries", 0), nat_sum.get("hoursworked", 0), 200000)
        state_sev = safe_div(st_sum.get("daysawayfromwork", 0), st_sum.get("hoursworked", 0), 200000)
        nat_sev = safe_div(nat_sum.get("daysawayfromwork", 0), nat_sum.get("hoursworked", 0), 200000)
        state_fat = safe_div(st_sum.get("fatalities", 0), st_sum.get("employees", 0), 100000)
        nat_fat = safe_div(nat_sum.get("fatalities", 0), nat_sum.get("employees", 0), 100000)

        c1, c2, c3 = st.columns(3)
        c1.metric("💥 TRIR", state_trir, f"National: {nat_trir}")
        c2.metric("📆 Severity", state_sev, f"National: {nat_sev}")
        c3.metric("☠️ Fatality Rate", state_fat, f"National: {nat_fat}")

        # Trend by year (injuries)
        df_trend = df_state.groupby("year", dropna=True)["injuries"].sum().reset_index()
        if not df_trend.empty:
            st.subheader(f"📈 Injury Trend in {state_choice}")
            fig_state_trend = px.line(
                df_trend, x="year", y="injuries", markers=True, labels={"year": "Year", "injuries": "Injuries"}
            )
            fig_state_trend.update_traces(hovertemplate="Year %{x}<br>Injuries: %{y:,}")
            st.plotly_chart(fig_state_trend, use_container_width=True)

        # Summary table by year
        df_table = (
            df_state.groupby("year", dropna=True)
            .agg(
                injuries=("injuries", "sum"),
                fatalities=("fatalities", "sum"),
                daysawayfromwork=("daysawayfromwork", "sum"),
                jobtransferrestriction=("jobtransferrestriction", "sum"),
                hoursworked=("hoursworked", "sum"),
                employees=("employees", "sum"),
            )
            .reset_index()
        )
        if not df_table.empty:
            df_table["TRIR (/200k hrs)"] = (df_table["injuries"] / df_table["hoursworked"]).fillna(0) * 200000
            df_table["Fatality Rate (/100k emp)"] = (df_table["fatalities"] / df_table["employees"]).fillna(0) * 100000

            st.subheader(f"📋 Summary for {state_choice}")
            st.dataframe(
                df_table.rename(
                    columns={
                        "year": "Year",
                        "injuries": "Injuries",
                        "fatalities": "Fatalities",
                        "daysawayfromwork": "Lost Days (DAFW)",
                        "jobtransferrestriction": "Work Restrictions (DJTR)",
                    }
                ),
                use_container_width=True,
            )

        st.info(
            """
**How to read these indicators:**
- **Lost Days (DAFW)** → total days lost due to injuries with absence from work (severity proxy).
- **TRIR (/200k hrs)** → injuries per 200,000 hours worked (~100 FTE-year).
"""
        )

# -------------------------------------------------------------------
# TAB 3 - SECTORS
# -------------------------------------------------------------------
with tab3:
    st.header("Sector Analysis")

    if df_sectors.empty or df_inc.empty:
        st.error("⚠️ Missing 'sectors' or 'incidents' data.")
    else:
        macros = sorted(df_sectors["sector_macro"].dropna().unique().tolist())
        sector_choice = st.selectbox("🏭 Select a Macro Sector:", macros)

        df_full = incidents_with_state_sector()
        df_full = df_full[df_full["sector_macro"] == sector_choice]

        # KPIs Sector vs National (all years)
        nat_sum = incidents_with_state_sector().sum(numeric_only=True)
        sec_sum = df_full.sum(numeric_only=True)

        sec_trir = safe_div(sec_sum.get("injuries", 0), sec_sum.get("hoursworked", 0), 200000)
        nat_trir = safe_div(nat_sum.get("injuries", 0), nat_sum.get("hoursworked", 0), 200000)
        sec_sev = safe_div(sec_sum.get("daysawayfromwork", 0), sec_sum.get("hoursworked", 0), 200000)
        nat_sev = safe_div(nat_sum.get("daysawayfromwork", 0), nat_sum.get("hoursworked", 0), 200000)
        sec_fat = safe_div(sec_sum.get("fatalities", 0), sec_sum.get("employees", 0), 100000)
        nat_fat = safe_div(nat_sum.get("fatalities", 0), nat_sum.get("employees", 0), 100000)

        c1, c2, c3 = st.columns(3)
        c1.metric("💥 TRIR", sec_trir, f"National: {nat_trir}")
        c2.metric("📆 Severity", sec_sev, f"National: {nat_sev}")
        c3.metric("☠️ Fatality Rate", sec_fat, f"National: {nat_fat}")

        # Top NAICS 3-digit by injuries (within selected macro sector)
        df_sub = df_full.copy()
        if not df_sub.empty and "naics_code" in df_sub.columns:
            df_sub["naics3"] = df_sub["naics_code"].astype(str).str[:3]
            top_sub = (
                df_sub.groupby("naics3", dropna=True)["injuries"]
                .sum()
                .reset_index()
                .sort_values("injuries", ascending=False)
                .head(10)
            )
            if not top_sub.empty:
                st.subheader(f"🏭 Top 10 Risky Sub-sectors in {sector_choice}")
                fig_sub = px.bar(
                    top_sub, x="naics3", y="injuries", labels={"naics3": "NAICS (3-digit)", "injuries": "Injuries"}
                )
                fig_sub.update_traces(hovertemplate="NAICS %{x}<br>Injuries: %{y:,}")
                st.plotly_chart(fig_sub, use_container_width=True)

                st.caption(
                    "NAICS: 2 digits = macro sector (e.g., 23 Construction), 3 digits = sub-sector (e.g., 236 Building Construction)."
                )

        # Injury rate (/1000 emp) by macro sector
        df_rate = incidents_with_state_sector()
        if not df_rate.empty:
            rate = (
                df_rate.groupby("sector_macro", dropna=True)
                .agg(injuries=("injuries", "sum"), employees=("employees", "sum"))
                .reset_index()
            )
            rate = rate[rate["employees"] > 0]
            rate["Incident Rate (/1000 emp)"] = (rate["injuries"] / rate["employees"]) * 1000
            rate = rate.sort_values("Incident Rate (/1000 emp)", ascending=False)

            if not rate.empty:
                st.subheader("⚖️ Injury Rate by Macro Sector (injuries / 1000 employees)")
                fig_rate = px.bar(
                    rate,
                    x="Incident Rate (/1000 emp)",
                    y="sector_macro",
                    orientation="h",
                    labels={"sector_macro": "Macro Sector"},
                )
                fig_rate.update_traces(hovertemplate="<b>%{y}</b><br>Rate: %{x}")
                st.plotly_chart(fig_rate, use_container_width=True)

# -------------------------------------------------------------------
# TAB 4 - COMBINED ANALYSIS
# -------------------------------------------------------------------
with tab4:
    st.header("Combined Analysis: State + Sector + Year")

    df_all = incidents_with_state_sector()
    if df_all.empty:
        st.error("⚠️ Missing data to combine.")
    else:
        years = sorted(df_all["year"].dropna().astype(int).unique().tolist())
        states = sorted(df_all["state_name"].dropna().unique().tolist())
        macros = sorted(df_all["sector_macro"].dropna().unique().tolist())

        year_sel = st.selectbox("📅 Select Year:", years, index=len(years) - 1 if years else 0)
        state_sel = st.selectbox("🗺️ Select State:", states, index=0 if states else None)
        sector_sel = st.selectbox("🏭 Select Macro Sector:", macros, index=0 if macros else None)

        df_f = df_all[
            (df_all["year"] == year_sel)
            & (df_all["state_name"] == state_sel)
            & (df_all["sector_macro"] == sector_sel)
        ]

        # Combined metrics table (single row)
        df_tbl = (
            df_f.agg(
                injuries=("injuries", "sum"),
                fatalities=("fatalities", "sum"),
                hoursworked=("hoursworked", "sum"),
                employees=("employees", "sum"),
            )
            .to_frame()
            .T
        )

        if not df_tbl.empty:
            df_tbl["TRIR (/200k hrs)"] = safe_div(
                df_tbl.loc[0, "injuries"], df_tbl.loc[0, "hoursworked"], 200000
            )
            st.subheader(f"📊 {state_sel} – {sector_sel} ({year_sel})")
            st.dataframe(
                pd.DataFrame(
                    {
                        "Year": [year_sel],
                        "State": [state_sel],
                        "Macro Sector": [sector_sel],
                        "Injuries": [
                            int(df_tbl.loc[0, "injuries"]) if pd.notna(df_tbl.loc[0, "injuries"]) else 0
                        ],
                        "Fatalities": [
                            int(df_tbl.loc[0, "fatalities"]) if pd.notna(df_tbl.loc[0, "fatalities"]) else 0
                        ],
                        "TRIR (/200k hrs)": [df_tbl.loc[0, "TRIR (/200k hrs)"]],
                    }
                ),
                use_container_width=True,
            )

            # Multi-year trend for chosen state + sector
            df_tr = (
                df_all[(df_all["state_name"] == state_sel) & (df_all["sector_macro"] == sector_sel)]
                .groupby("year", dropna=True)["injuries"]
                .sum()
                .reset_index()
            )
            if not df_tr.empty:
                st.subheader(f"📈 Injury Trend – {state_sel} ({sector_sel})")
                fig_tr = px.line(
                    df_tr, x="year", y="injuries", markers=True, labels={"year": "Year", "injuries": "Injuries"}
                )
                fig_tr.update_traces(hovertemplate="Year %{x}<br>Injuries: %{y:,}")
                st.plotly_chart(fig_tr, use_container_width=True)

            # KPI gauge vs national (same year)
            df_nat_year = df_all[df_all["year"] == year_sel]
            val = safe_div(df_tbl.loc[0, "injuries"], df_tbl.loc[0, "hoursworked"], 200000)
            ref = safe_div(df_nat_year["injuries"].sum(), df_nat_year["hoursworked"].sum(), 200000)

            st.subheader("📌 TRIR vs National Average")
            max_val = max(val, ref)
            gauge_max = max_val * 1.5 if max_val > 0 else 1
            fig_kpi = go.Figure(
                go.Indicator(
                    mode="gauge+number+delta",
                    value=val,
                    delta={"reference": ref, "increasing": {"color": "red"}, "decreasing": {"color": "green"}},
                    gauge={
                        "axis": {"range": [0, gauge_max]},
                        "bar": {"color": "blue"},
                        "steps": [
                            {"range": [0, ref], "color": "lightgreen"},
                            {"range": [ref, gauge_max], "color": "pink"},
                        ],
                    },
                    title={"text": f"TRIR {state_sel} – {sector_sel} ({year_sel})"},
                )
            )
            st.plotly_chart(fig_kpi, use_container_width=True)

            # Scenario Simulator
            st.subheader("🧪 Scenario Simulator")
            cA, cB, cC = st.columns(3)
            with cA:
                delta_emp = st.slider("Change in Employees (%)", -30, 30, 0, step=5)
            with cB:
                delta_hours = st.slider("Change in Hours Worked (%)", -30, 30, 0, step=5)
            with cC:
                delta_inj = st.slider("Change in Injuries (%)", -50, 50, 0, step=5)

            injuries = float(df_tbl.loc[0, "injuries"] or 0)
            fatalities = float(df_tbl.loc[0, "fatalities"] or 0)
            employees = float(df_tbl.loc[0, "employees"] or 0)
            hours = float(df_tbl.loc[0, "hoursworked"] or 0)

            emp_adj = max(employees * (1 + delta_emp / 100), 1.0) if employees else 0.0
            hrs_adj = max(hours * (1 + delta_hours / 100), 1.0) if hours else 0.0
            inj_adj = max(injuries * (1 + delta_inj / 100), 0.0)

            trir_orig = safe_div(injuries, hours, 200000)
            fat_orig = safe_div(fatalities, employees, 100000)
            trir_new = safe_div(inj_adj, hrs_adj, 200000)
            fat_new = safe_div(fatalities, emp_adj, 100000)

            d1, d2 = st.columns(2)
            with d1:
                st.metric("💥 TRIR (original)", trir_orig)
                st.metric("☠️ Fatality Rate (original)", fat_orig)
            with d2:
                st.metric("💥 TRIR (simulated)", trir_new, f"{trir_new - trir_orig:+.2f}")
                st.metric("☠️ Fatality Rate (simulated)", fat_new, f"{fat_new - fat_orig:+.2f}")

            st.info(
                """
**How to interpret the simulator**
- Sliders apply % changes to denominators/numerators:  
  • Employees → denominator of Fatality Rate  
  • Hours worked → denominator of TRIR  
  • Injuries (%) → numerator of TRIR  
- This is a *what-if* tool; recorded fatalities remain historical.
"""
            )
        else:
            st.warning("No data for selected filters.")

# -------------------------------------------------------------------
# TAB 5 - INSIGHTS & EXPORT
# -------------------------------------------------------------------
with tab5:
    st.header("Insights & Reporting")

    df_all = incidents_with_state_sector()
    if df_all.empty:
        st.error("⚠️ df_all is empty after merge.")
    else:
        latest_year = int(df_all["year"].dropna().max()) if not df_all["year"].dropna().empty else None
        st.caption(f"Latest year in dataset: **{latest_year}**" if latest_year else "No year information available.")

        if latest_year:
            df_y = df_all[df_all["year"] == latest_year]

            # Top 10 states by TRIR
            states_rate = (
                df_y.groupby("state_name", dropna=True)
                .agg(injuries=("injuries", "sum"), hours=("hoursworked", "sum"))
                .reset_index()
            )
            states_rate = states_rate[states_rate["hours"] > 0]
            states_rate["TRIR"] = (states_rate["injuries"] / states_rate["hours"]) * 200000
            states_rate = states_rate.sort_values("TRIR", ascending=False).head(10)

            if not states_rate.empty:
                st.subheader(f"🔥 Top 10 States by TRIR ({latest_year})")
                fig_s = px.bar(
                    states_rate,
                    x="TRIR",
                    y="state_name",
                    orientation="h",
                    labels={"TRIR": "TRIR", "state_name": "State"},
                )
                fig_s.update_traces(hovertemplate="<b>%{y}</b><br>TRIR: %{x:.2f}")
                st.plotly_chart(fig_s, use_container_width=True)

            # Top 10 macro sectors by TRIR
            sectors_rate = (
                df_y.groupby("sector_macro", dropna=True)
                .agg(injuries=("injuries", "sum"), hours=("hoursworked", "sum"))
                .reset_index()
            )
            sectors_rate = sectors_rate[sectors_rate["hours"] > 0]
            sectors_rate["TRIR"] = (sectors_rate["injuries"] / sectors_rate["hours"]) * 200000
            sectors_rate = sectors_rate.sort_values("TRIR", ascending=False).head(10)

            if not sectors_rate.empty:
                st.subheader(f"🏭 Top 10 Macro Sectors by TRIR ({latest_year})")
                fig_m = px.bar(
                    sectors_rate,
                    x="TRIR",
                    y="sector_macro",
                    orientation="h",
                    labels={"TRIR": "TRIR", "sector_macro": "Macro Sector"},
                )
                fig_m.update_traces(hovertemplate="<b>%{y}</b><br>TRIR: %{x:.2f}")
                st.plotly_chart(fig_m, use_container_width=True)

        # -------- Export (Excel / PDF) --------
        st.subheader("📥 Export (Excel / PDF)")

        # Filter pickers
        all_years = sorted(df_all["year"].dropna().astype(int).unique().tolist())
        all_states = sorted(df_regions["state_name"].dropna().unique().tolist())
        all_macros = sorted(df_sectors["sector_macro"].dropna().unique().tolist())

        c1, c2, c3 = st.columns(3)
        with c1:
            exp_year = st.selectbox("Year (optional)", [None] + all_years, index=0)
        with c2:
            exp_state = st.selectbox("State (optional)", [None] + all_states, index=0)
        with c3:
            exp_sector = st.selectbox("Macro Sector (optional)", [None] + all_macros, index=0)

        df_r = df_all.copy()
        if exp_year is not None:
            df_r = df_r[df_r["year"] == exp_year]
        if exp_state is not None:
            df_r = df_r[df_r["state_name"] == exp_state]
        if exp_sector is not None:
            df_r = df_r[df_r["sector_macro"] == exp_sector]

        if not df_r.empty:
            rep = (
                df_r.groupby(["year", "state_name", "sector_macro"], dropna=True)
                .agg(injuries=("injuries", "sum"), fatalities=("fatalities", "sum"), hours=("hoursworked", "sum"))
                .reset_index()
                .rename(
                    columns={
                        "year": "Year",
                        "state_name": "State",
                        "sector_macro": "Macro Sector",
                        "injuries": "Injuries",
                        "fatalities": "Fatalities",
                        "hours": "Hours",
                    }
                )
            )
            rep["TRIR (/200k hrs)"] = (rep["Injuries"] / rep["Hours"]).fillna(0) * 200000
            rep = rep.drop(columns=["Hours"])

            # Excel
            buffer_xlsx = io.BytesIO()
            with pd.ExcelWriter(buffer_xlsx, engine="xlsxwriter") as writer:
                rep.to_excel(writer, index=False, sheet_name="HSE Report")
            st.download_button(
                label="⬇️ Download Excel",
                data=buffer_xlsx.getvalue(),
                file_name="hse_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            # PDF
            buffer_pdf = io.BytesIO()
            doc = SimpleDocTemplate(buffer_pdf, pagesize=A4)
            styles = getSampleStyleSheet()
            elements = []
            elements.append(Paragraph("📊 HSE Report", styles["Title"]))
            elements.append(Spacer(1, 8))
            filt = f"Filters — Year: {exp_year or 'All'} | State: {exp_state or 'All'} | Sector: {exp_sector or 'All'}"
            elements.append(Paragraph(filt, styles["Normal"]))
            elements.append(Spacer(1, 8))

            data = [rep.columns.tolist()] + rep.astype(str).values.tolist()
            table = Table(data, repeatRows=1)
            table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.lightblue),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                        ("FONTSIZE", (0, 0), (-1, -1), 8),
                        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey]),
                    ]
                )
            )
            elements.append(table)
            doc.build(elements)

            st.download_button(
                label="⬇️ Download PDF",
                data=buffer_pdf.getvalue(),
                file_name="hse_report.pdf",
                mime="application/pdf",
            )
        else:
            st.warning("No data available for the selected export filters.")