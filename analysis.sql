-- ==============================
-- ANALISI OSHA INCIDENT DATA
-- ==============================

-- 1. Verifica dati caricati
SELECT COUNT(*) AS total_records FROM incidents;
SELECT COUNT(*) AS total_sectors FROM sectors;
SELECT COUNT(*) AS total_regions FROM regions;

-- 2. Trend annuale (infortuni + fatalità)
SELECT year,
       SUM(injuries)   AS total_injuries,
       SUM(fatalities) AS total_fatalities
FROM incidents
GROUP BY year
ORDER BY year;

-- 3. Top 10 settori per numero di infortuni
SELECT s.sector_clean AS sector,
       SUM(i.injuries) AS total_injuries
FROM incidents i
JOIN sectors s ON i.naics_code = s.naics_code
GROUP BY s.sector_clean
ORDER BY total_injuries DESC
LIMIT 10;

-- 4. Incident rate per 1000 dipendenti
SELECT s.sector_clean AS sector,
       i.year,
       ROUND(SUM(i.injuries)::decimal / NULLIF(SUM(i.employees),0) * 1000, 2) AS incident_rate
FROM incidents i
JOIN sectors s ON i.naics_code = s.naics_code
GROUP BY s.sector_clean, i.year
ORDER BY incident_rate DESC
LIMIT 20;

-- 5. Distribuzione geografica: top 10 stati per infortuni
SELECT r.state_name,
       SUM(i.injuries) AS total_injuries
FROM incidents i
JOIN regions r ON i.state_code = r.state_code
GROUP BY r.state_name
ORDER BY total_injuries DESC
LIMIT 10;

-- 6. Rapporto Fatalità / Infortuni per settore
SELECT s.sector_clean AS sector,
       ROUND(SUM(i.fatalities)::decimal / NULLIF(SUM(i.injuries),0), 3) AS fatality_ratio
FROM incidents i
JOIN sectors s ON i.naics_code = s.naics_code
GROUP BY s.sector_clean
ORDER BY fatality_ratio DESC
LIMIT 10;