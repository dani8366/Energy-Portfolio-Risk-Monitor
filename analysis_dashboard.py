import pandas as pd
from sqlalchemy import create_engine

# --- CONFIG ---
DB_USER = 'root'
DB_PASSWORD = 'HIER_DEIN_PASSWORT_EINTRAGEN'  # <--- HIER PASSWORT EINTRAGEN
DB_HOST = 'localhost'
DB_NAME = 'energy_risk_year_db'

connection_string = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(connection_string)


def analyze_portfolio_risk():
    print("--- Starte Portfolio-Analyse (SQL Join über 4 Tabellen) ---")

    # Der komplexe SQL-Query, der die Realität abbildet.
    # Wir joinen Preise, Last und Erzeugung über den Zeitstempel.
    # Trades werden über eine Subquery aggregiert (da sie Zeiträume sind).
    query = """
    SELECT 
        p.datum,
        p.stunde,
        p.price_eur_mwh as spot_price,
        l.load_mw,
        g.solar_mw,
        g.wind_mw,
        -- Aggregiere alle Trades, die in diesem Zeitraum aktiv sind (Buy)
        (
            SELECT COALESCE(SUM(menge_mw), 0)
            FROM trades t
            WHERE p.datum BETWEEN t.start_datum AND t.end_datum
            AND t.typ = 'Buy'
        ) as bought_mw
    FROM spot_prices p
    JOIN grid_load l ON p.datum = l.datum AND p.stunde = l.stunde
    JOIN generation_assets g ON p.datum = g.datum AND p.stunde = g.stunde
    ORDER BY p.datum, p.stunde
    """

    print("Lade Daten aus MySQL...")
    df = pd.read_sql(query, con=engine)

    # --- PANDAS BERECHNUNG ---

    # 1. Erneuerbare Gesamt
    df['renewables_mw'] = df['solar_mw'] + df['wind_mw']

    # 2. Portfolio Supply (Was haben wir zur Verfügung?)
    # Gekaufte Trades + Eigene Erneuerbare
    df['total_supply_mw'] = df['bought_mw'] + df['renewables_mw']

    # 3. Open Position (Long/Short)
    # Supply - Demand (Load)
    # Positiv = Long (Überschuss, muss verkauft werden)
    # Negativ = Short (Mangel, muss nachgekauft werden)
    df['open_position_mw'] = df['total_supply_mw'] - df['load_mw']

    # 4. Bewertung der Open Position (Mark-to-Market)
    # Wir bewerten die offene Position mit dem aktuellen stündlichen Spotpreis
    df['mtm_spot_value_eur'] = df['open_position_mw'] * df['spot_price']

    # --- KPI REPORTING ---
    print("\n" + "=" * 40)
    print("   JAHRESREPORT 2026 - RISIKO ANALYSE")
    print("=" * 40)

    total_load_gwh = df['load_mw'].sum() / 1000
    total_ee_gwh = df['renewables_mw'].sum() / 1000
    total_trade_gwh = df['bought_mw'].sum() / 1000

    print(f"Gesamtverbrauch (Last):       {total_load_gwh:,.0f} GWh")
    print(f"Beschaffung Terminmarkt:      {total_trade_gwh:,.0f} GWh")
    print(f"Eigen-Erzeugung (Wind/PV):    {total_ee_gwh:,.0f} GWh")
    print(f" -> EE-Quote im Portfolio:    {(total_ee_gwh / total_load_gwh) * 100:.1f} %")

    print("-" * 40)

    # Finanzielle Auswertung
    # 1. Kosten für die Trades (Fixkosten aus Terminmarkt)
    trades_df = pd.read_sql("SELECT * FROM trades", engine)
    # Berechnung: Menge * Stunden * Preis (grob geschätzt über Tagesdifferenz * 24)
    # In SQL DATEDIFF gibt Tage zurück, +1 um inklusiv zu sein
    trade_cost_query = """
    SELECT SUM(menge_mw * (DATEDIFF(end_datum, start_datum) + 1) * 24 * preis_eur_mwh) 
    FROM trades WHERE typ='Buy'
    """
    fixed_costs = pd.read_sql(trade_cost_query, engine).iloc[0, 0]

    # 2. Ergebnis Spotmarkt (Variable Kosten/Erlöse)
    # Summe von mtm_spot_value_eur:
    # Wenn wir Long sind (Positiv), verkaufen wir -> Einnahme
    # Wenn wir Short sind (Negativ), kaufen wir -> Ausgabe (Negativ * Preis = Negativ)
    spot_result = df['mtm_spot_value_eur'].sum()

    print(f"Fixkosten (Terminmarkt):      {fixed_costs / 1e6:,.2f} Mio. EUR")
    print(f"Ergebnis Spotmarkt (Ausgleich): {spot_result / 1e6:,.2f} Mio. EUR")
    print(f" -> (Positiv = Netto-Erlös durch Überschuss-Verkauf)")

    total_energy_cost = fixed_costs - spot_result
    avg_price = total_energy_cost / (total_load_gwh * 1000)

    print("-" * 40)
    print(f"GESAMTKOSTEN ENERGIE:         {total_energy_cost / 1e6:,.2f} Mio. EUR")
    print(f"DURCHSCHNITTSPREIS:           {avg_price:.2f} EUR/MWh")
    print("=" * 40)

    # Export
    filename = "Risk_Report_Modular_DB.xlsx"
    df.to_excel(filename, index=False)
    print(f"\n✅ Detaillierter Report gespeichert als '{filename}'")


if __name__ == "__main__":
    analyze_portfolio_risk()