import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# --- CONFIG ---
DB_USER = 'root'
DB_PASSWORD = 'HIER_DEIN_PASSWORT_EINTRAGEN'  # <--- HIER PASSWORT EINTRAGEN
DB_HOST = 'localhost'
DB_NAME = 'energy_risk_year_db'

# SQLAlchemy Connection String für MySQL
connection_string = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(connection_string)


# --- 1. SIMULATION ENGINE (Erzeugt Rohdaten) ---

def generate_full_year_scenarios(year=2026):
    print(f"--- 1. Generiere Szenario-Daten für Jahr {year} ---")

    # Zeitindex für das ganze Jahr (stündlich)
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31 23:00"
    date_rng = pd.date_range(start=start_date, end=end_date, freq='h')
    n = len(date_rng)

    # A. LASTPROFIL (Verbrauch)
    day_of_year = date_rng.dayofyear
    hour_of_day = date_rng.hour

    # Saisonalität (Winter hoch) + Tagesprofil (M-Profil) + Rauschen
    seasonality = 50 + 20 * np.cos((day_of_year - 20) / 365 * 2 * np.pi)
    daily_pattern = 10 * (np.sin((hour_of_day - 6) / 24 * 2 * np.pi) + 0.5 * np.sin((hour_of_day - 9) / 12 * 2 * np.pi))
    load_profile = np.maximum(seasonality + daily_pattern + np.random.normal(0, 2, n), 20)

    # B. SOLAR (Sommer hoch, Tagsüber Glocke)
    solar_season = np.maximum(0, -np.cos((day_of_year) / 365 * 2 * np.pi))
    solar_daily = np.maximum(0, -((hour_of_day - 12) ** 2) / 10 + 1)
    solar_profile = solar_season * solar_daily * 45 * np.random.uniform(0.2, 1.0, n)  # Max 45 MW

    # C. WIND (Winter hoch, Stochastisch)
    wind_raw = np.cumsum(np.random.normal(0, 1, n))
    wind_profile = (wind_raw - wind_raw.min()) / (wind_raw.max() - wind_raw.min()) * 55
    wind_season_factor = 1 + 0.5 * np.cos((day_of_year - 20) / 365 * 2 * np.pi)
    wind_profile = wind_profile * wind_season_factor

    # D. PREIS (Merit-Order Simulation)
    # Residual Load = Last - (Wind + Solar)
    # Preis korreliert stark mit der Residual Load
    residual_load = load_profile - (solar_profile + wind_profile)
    market_price = 45 + 1.8 * residual_load + np.random.normal(0, 5, n)

    # Alles in einen DataFrame packen (vorerst)
    df = pd.DataFrame({
        'datum': date_rng.date,
        'stunde': date_rng.hour,
        'price_eur_mwh': market_price,
        'load_mw': load_profile,
        'solar_mw': solar_profile,
        'wind_mw': wind_profile
    })

    return df


def book_strategic_trades(year=2026):
    print("--- 2. Buche Handelsgeschäfte (Trades) ---")
    trades = []

    # Trade 1: Base Load (Jahresband) - Deckt die Grundlast
    trades.append({
        'trade_id': 'TRD_YEAR_BASE_26',
        'produkt': 'Cal-26 Base',
        'typ': 'Buy',
        'menge_mw': 35.0,
        'preis_eur_mwh': 90.0,
        'start_datum': f'{year}-01-01',
        'end_datum': f'{year}-12-31'
    })

    # Trade 2: Winter Peak (Q1) - Deckt Heizlast
    trades.append({
        'trade_id': 'TRD_Q1_PEAK_26',
        'produkt': 'Q1-26 Peak',
        'typ': 'Buy',
        'menge_mw': 15.0,
        'preis_eur_mwh': 115.0,
        'start_datum': f'{year}-01-01',
        'end_datum': f'{year}-03-31'
    })

    # Trade 3: Winter Peak (Q4)
    trades.append({
        'trade_id': 'TRD_Q4_PEAK_26',
        'produkt': 'Q4-26 Peak',
        'typ': 'Buy',
        'menge_mw': 15.0,
        'preis_eur_mwh': 120.0,
        'start_datum': f'{year}-10-01',
        'end_datum': f'{year}-12-31'
    })

    return pd.DataFrame(trades)


# --- 2. ETL PROZESS (Verteilen auf Tabellen) ---

def run_etl_pipeline():
    # Daten generieren
    df_sim = generate_full_year_scenarios(2026)
    df_trades = book_strategic_trades(2026)

    print("\n--- Starte Upload in modulare SQL-Struktur ---")

    # A. SPOT PREISE (Nur Datum, Stunde, Preis)
    # Wir nutzen if_exists='append', da die Tabellen schon durch dein SQL-Skript existieren.
    # Wichtig: Spaltennamen müssen exakt mit SQL übereinstimmen!
    df_prices = df_sim[['datum', 'stunde', 'price_eur_mwh']]
    df_prices.to_sql('spot_prices', con=engine, if_exists='append', index=False)
    print("✅ Tabelle 'spot_prices' gefüllt.")

    # B. VERBRAUCH / LAST
    df_load = df_sim[['datum', 'stunde', 'load_mw']]
    df_load.to_sql('grid_load', con=engine, if_exists='append', index=False)
    print("✅ Tabelle 'grid_load' gefüllt.")

    # C. ERZEUGUNG / ASSETS
    df_assets = df_sim[['datum', 'stunde', 'solar_mw', 'wind_mw']]
    df_assets.to_sql('generation_assets', con=engine, if_exists='append', index=False)
    print("✅ Tabelle 'generation_assets' gefüllt.")

    # D. TRADES
    df_trades.to_sql('trades', con=engine, if_exists='append', index=False)
    print("✅ Tabelle 'trades' gefüllt.")

    print("\n🎉 ETL-Prozess erfolgreich abgeschlossen! Die Datenbank ist bereit.")


if __name__ == "__main__":
    try:
        run_etl_pipeline()
    except Exception as e:
        print(f"\n❌ FEHLER: {e}")
        print("Tipp: Überprüfe, ob die Tabellen in MySQL Workbench mit dem SQL-Skript erstellt wurden.")