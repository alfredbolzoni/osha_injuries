# 📊 OSHA Workplace Injuries Dashboard

An interactive **data analytics dashboard** built with **Streamlit**, **PostgreSQL**, and **Plotly**, providing insights into workplace injuries across U.S. states and industrial sectors.  
The project allows users to explore injury trends, benchmark performance, and simulate scenarios to support **HSE (Health, Safety & Environment) management decisions**.

---

## ✨ Features

### 1. National Overview
- 📌 **Key Safety Indicators**:
  - **TRIR (Total Recordable Incident Rate)** → Injuries per 200,000 hours worked.
  - **Severity Rate** → Lost days (DAFW) per 200,000 hours worked.
  - **Fatality Rate** → Deaths per 100,000 employees.
- 📈 **Trends over time** with year-to-year deltas.
- 🗺️ **Geographical distribution** of injuries across U.S. states.

### 2. State Analysis
- Compare a **selected state vs national averages**.
- 📈 Injury trends per year.
- 📋 Summary tables with **injuries, fatalities, lost days (DAFW), job restrictions (DJTR), TRIR, and fatality rates**.
- 🔎 Contextual explanations to improve interpretation.

### 3. Sector Analysis
- Explore **macro-sectors** (Construction, Manufacturing, Healthcare, etc.).
- 📊 KPI comparison with U.S. averages.
- 🏭 Top 10 **sub-sectors (NAICS 3-digit)** most at risk.
- ⚖️ Incident rate (injuries per 1,000 employees).
- 📊 OSHA benchmark reference values.

### 4. Combined Analysis (State + Sector + Year)
- Custom selection: **State + Sector + Year**.
- 📋 Detailed table of key metrics.
- 📈 Multi-year trend analysis.
- 📌 **TRIR vs National Average** with interactive **gauge chart**.
- 🧪 **Scenario Simulator**:
  - Adjust % of employees and hours worked.
  - Observe how TRIR and Fatality Rate change under simulated conditions.
- 📥 **Downloadable Reports**:
  - Excel (`.xlsx`)  
  - PDF (`.pdf`)

---

## 🛠️ Tech Stack

- **Frontend**: [Streamlit](https://streamlit.io/)  
- **Database**: PostgreSQL (with `psycopg2`)  
- **Visualization**: Plotly (Express & Graph Objects)  
- **Exporting**: `xlsxwriter` (Excel), `reportlab` (PDF)  

---

## 📂 Project Structure