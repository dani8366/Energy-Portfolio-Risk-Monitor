Sql: -- 1. MARKTPREISE (Quelle: EEX / Spotmarkt)-- Enthält nur finanzielle ZeitreihenCREATE TABLE spot_prices (
datum DATE,
stunde INT,
price_eur_mwh DECIMAL(10, 2),
PRIMARY KEY (datum, stunde)
);-- 2. VERBRAUCH / LAST (Quelle: EDM System / Metering)-- Das aggregierte Profil aller Kunden im BKVCREATE TABLE grid_load (
datum DATE,
stunde INT,
load_mw DECIMAL(10, 2),
PRIMARY KEY (datum, stunde)
);-- 3. ERZEUGUNG / ASSETS (Quelle: Leittechnik / Wetterdienst)-- Wir trennen Solar und Wind, damit wir sie einzeln analysieren können.CREATE TABLE generation_assets (
datum DATE,
stunde INT,
solar_mw DECIMAL(10, 2),
wind_mw DECIMAL(10, 2),
PRIMARY KEY (datum, stunde)
);-- 4. HANDELSBUCH (Quelle: ETRM System)-- Unsere Verträge (Base, Peak, etc.)CREATE TABLE trades (
trade_id VARCHAR(50) PRIMARY KEY,
produkt VARCHAR(50),
typ VARCHAR(10), -- 'Buy' / 'Sell'
menge_mw DECIMAL(10, 2),
preis_eur_mwh DECIMAL(10, 2),
start_datum DATE,
end_datum DATE
);
