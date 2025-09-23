import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# === 1. Carica CSV già pulito ===
print("Caricamento CSV pulito...")
df = pd.read_csv("data/osha_clean_final.csv")

print("Shape iniziale:", df.shape)

# === 2. Funzione per validare NAICS ===
def is_valid_naics(value):
    try:
        s = str(int(value))
        return 2 <= len(s) <= 6   # NAICS ha da 2 a 6 cifre
    except:
        return False

# === 3. Filtra i record con NAICS valido ===
df = df[df["NAICS"].apply(is_valid_naics)].copy()
print("Shape dopo filtro NAICS validi:", df.shape)

# === 4. Connessione a Postgres ===
conn = psycopg2.connect(
    dbname="osha",
    user="postgres",               # <-- aggiorna se usi altro utente
    password="alfred1993",    # <-- metti la password
    host="localhost",
    port=5433
)
cur = conn.cursor()

# === 5. Inserisci stati (regions) ===
regions = df[["State", "StateName"]].drop_duplicates()
execute_values(
    cur,
    """
    INSERT INTO regions (state_code, state_name)
    VALUES %s
    ON CONFLICT (state_code) DO NOTHING;
    """,
    regions.values.tolist()
)

# === 6. Inserisci settori (sectors) ===
sectors = df[["NAICS", "Sector", "SectorClean", "SectorMacro"]].drop_duplicates()
execute_values(
    cur,
    """
    INSERT INTO sectors (naics_code, sector_name, sector_clean, sector_macro)
    VALUES %s
    ON CONFLICT (naics_code) DO NOTHING;
    """,
    sectors.values.tolist()
)

# === 7. Inserisci incidenti (incidents) ===
records = []
for _, row in df.iterrows():
    year = int(row["Year"])
    state = row["State"]          # già validato
    naics = row["NAICS"]

    employees = int(row["Employees"]) if pd.notna(row["Employees"]) else None
    hours = int(row["HoursWorked"]) if pd.notna(row["HoursWorked"]) else None
    injuries = int(row["Injuries"]) if pd.notna(row["Injuries"]) else 0
    fatalities = int(row["Fatalities"]) if pd.notna(row["Fatalities"]) else 0
    dafw = int(row["DaysAwayFromWork"]) if pd.notna(row["DaysAwayFromWork"]) else 0
    jtr = int(row["JobTransferRestriction"]) if pd.notna(row["JobTransferRestriction"]) else 0
    other = int(row["OtherCases"]) if pd.notna(row["OtherCases"]) else 0

    records.append((year, state, naics, employees, hours,
                    injuries, fatalities, dafw, jtr, other))

execute_values(
    cur,
    """
    INSERT INTO incidents
    (year, state_code, naics_code, employees, hoursworked,
     injuries, fatalities, daysawayfromwork, jobtransferrestriction, othercases)
    VALUES %s;
    """,
    records
)

# === 8. Commit & chiudi ===
conn.commit()
cur.close()
conn.close()
print("✅ ETL completato: dati caricati in Postgres")