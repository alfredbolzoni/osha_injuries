import pandas as pd

print("Caricamento dataset originale...")
df = pd.read_csv("data/ITA_OSHA_Combined.csv", low_memory=False)

print("Shape originale:", df.shape)

# === 1. Selezione colonne utili ===
cols_keep = [
    "year_filing_for", "state", "naics_code", "industry_description",
    "annual_average_employees", "total_hours_worked",
    "total_injuries", "total_deaths",
    "total_dafw_cases", "total_djtr_cases", "total_other_cases"
]

df = df[cols_keep].copy()

# === 2. Rinomina colonne per chiarezza ===
df = df.rename(columns={
    "year_filing_for": "Year",
    "state": "State",
    "naics_code": "NAICS",
    "industry_description": "Sector",
    "annual_average_employees": "Employees",
    "total_hours_worked": "HoursWorked",
    "total_injuries": "Injuries",
    "total_deaths": "Fatalities",
    "total_dafw_cases": "DaysAwayFromWork",
    "total_djtr_cases": "JobTransferRestriction",
    "total_other_cases": "OtherCases"
})

# === 3. Pulizia valori ===
df = df.dropna(subset=["Year", "State", "NAICS", "Sector"])
df = df.drop_duplicates()

# Anno come intero
df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")

# Settore uppercase
df["Sector"] = df["Sector"].astype(str).str.strip().str.upper()

print("Shape dopo cleaning:", df.shape)

# === 4. Esportazione full pulito ===
df.to_csv("data/osha_clean_full.csv", index=False)
print("File completo pulito salvato in data/osha_clean_full.csv")

# === 5. Creazione sample stratificato ===
df_sample = (
    df.groupby(["Year", "Sector"], group_keys=False)
      .apply(lambda x: x.sample(min(len(x), 200), random_state=42))
)

df_sample.to_csv("data/osha_clean_sample.csv", index=False)
print("File sample salvato in data/osha_clean_sample.csv")
print("Shape sample:", df_sample.shape)

# === 6. Statistiche veloci per validazione ===
print("\n--- Statistiche veloci ---")
print("Anni disponibili:", df['Year'].unique())
print("Totale infortuni (sommati):", df['Injuries'].sum())
print("Totale fatalità (sommate):", df['Fatalities'].sum())
print("Top 5 settori per infortuni:")
print(df.groupby("Sector")["Injuries"].sum().sort_values(ascending=False).head(5))