import pandas as pd

# === 1. Carica il sample già pulito ===
df = pd.read_csv("data/osha_clean_sample.csv")

# === 2. Lista stati validi (50 + DC) ===
valid_states = set([
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "DC"
])

# === 3. Mapping codici → nomi completi (completo) ===
state_map = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia"
}

# === 4. Filtra solo stati validi ===
df = df[df["State"].isin(valid_states)].copy()
df["StateName"] = df["State"].map(state_map)

# === 5. Pulizia settori ===
def clean_sector_name(name):
    if pd.isna(name):
        return "Unknown"
    name = str(name).strip('"').title()
    name = name.replace("And", "&")
    name = " ".join(name.split())
    return name

df["SectorClean"] = df["Sector"].apply(clean_sector_name)

# === 6. Macrosettori da NAICS (prime 2 cifre) ===
naics_map = {
    "11": "Agriculture, Forestry, Fishing & Hunting",
    "21": "Mining, Quarrying, Oil & Gas Extraction",
    "22": "Utilities",
    "23": "Construction",
    "31": "Manufacturing", "32": "Manufacturing", "33": "Manufacturing",
    "42": "Wholesale Trade",
    "44": "Retail Trade", "45": "Retail Trade",
    "48": "Transportation & Warehousing", "49": "Transportation & Warehousing",
    "51": "Information",
    "52": "Finance & Insurance",
    "53": "Real Estate & Rental",
    "54": "Professional, Scientific & Technical Services",
    "55": "Management of Companies",
    "56": "Administrative & Support Services",
    "61": "Educational Services",
    "62": "Health Care & Social Assistance",
    "71": "Arts, Entertainment & Recreation",
    "72": "Accommodation & Food Services",
    "81": "Other Services",
    "92": "Public Administration"
}

def map_naics_to_macro(naics):
    try:
        code = str(int(naics))[:2]
        return naics_map.get(code, "Other")
    except:
        return "Other"

df["SectorMacro"] = df["NAICS"].apply(map_naics_to_macro)

# === 7. Salva file finale ===
df.to_csv("data/osha_clean_final.csv", index=False)

print("✅ File pulito salvato in data/osha_clean_final.csv")
print("Totale righe dopo filtro stati validi:", len(df))
print("Esempio righe pulite:")
print(df[["Year","State","StateName","SectorClean","SectorMacro"]].head(15))