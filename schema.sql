-- Pulizia vecchie tabelle
DROP TABLE IF EXISTS incidents CASCADE;
DROP TABLE IF EXISTS sectors CASCADE;
DROP TABLE IF EXISTS regions CASCADE;

-- ==============================
-- REGIONS (stati USA)
-- ==============================
CREATE TABLE regions (
    state_code VARCHAR(2) PRIMARY KEY CHECK (char_length(state_code) = 2),
    state_name TEXT NOT NULL
);

-- ==============================
-- SECTORS (settori NAICS)
-- ==============================
CREATE TABLE sectors (
    naics_code VARCHAR(10) PRIMARY KEY,
    sector_name TEXT,      -- originale sporco
    sector_clean TEXT,     -- pulito leggibile
    sector_macro TEXT NOT NULL  -- macrosettore NAICS
);

-- ==============================
-- INCIDENTS (incidenti registrati)
-- ==============================
CREATE TABLE incidents (
    incident_id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    state_code VARCHAR(2) REFERENCES regions(state_code) ON DELETE RESTRICT,
    naics_code VARCHAR(10) REFERENCES sectors(naics_code) ON DELETE RESTRICT,
    employees INT,
    hoursworked BIGINT,
    injuries INT DEFAULT 0,
    fatalities INT DEFAULT 0,
    daysawayfromwork INT DEFAULT 0,
    jobtransferrestriction INT DEFAULT 0,
    othercases INT DEFAULT 0
);