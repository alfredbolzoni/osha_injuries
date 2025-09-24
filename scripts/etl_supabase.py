# scripts/etl_supabase.py
import pandas as pd
from sqlalchemy import create_engine, text

# === 1. Load local CSV ===
df = pd.read_csv("/Users/alfredbolzoni/Documents/Projects/osha_injuries/data/osha_clean_final.csv")

# Normalize column names
df = df.rename(columns={
    "Year": "year",
    "State": "state_code",
    "NAICS": "naics_code",
    "Injuries": "injuries",
    "Fatalities": "fatalities",
    "HoursWorked": "hoursworked",
    "Employees": "employees",
    "DaysAwayFromWork": "daysawayfromwork",
    "JobTransferRestriction": "jobtransferrestriction"
})

# Keep only the columns that exist in Supabase "incidents"
keep_cols = ["year", "state_code", "naics_code", "injuries", "fatalities",
             "hoursworked", "employees", "daysawayfromwork", "jobtransferrestriction"]
df = df[keep_cols]

print("✅ File loaded:", df.shape, "rows/columns")
print("Years found:", sorted(df["year"].unique()))

# === 2. Connect to Supabase ===
engine = create_engine(
    "postgresql://postgres.qlgqlgrupwspehmjwgvm:Freddomala1993%24@aws-1-eu-central-2.pooler.supabase.com:5432/postgres"
)

# === 3. Truncate incidents table ===
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE incidents;"))

print("🧹 Table incidents truncated.")

# === 4. Upload data ===
df.to_sql("incidents", engine, if_exists="append", index=False)

print("🚀 Upload complete:", len(df), "rows inserted into Supabase.")