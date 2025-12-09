# Energy-Portfolio-Risk-Monitor
Automatisierte Risikoanalyse für Energieportfolios (Python &amp; SQL)
# Energy Portfolio Risk Monitor ⚡

Ein Python-basiertes Tool für das Risikocontrolling im Energiehandel. 
Es simuliert die Interaktion zwischen volatiler Einspeisung (Wind/Solar) und Handelsstrategien (Base/Peak), um das finanzielle Risiko (Mark-to-Market) zu quantifizieren.

## 🎯 Projektziel
Im modernen Energiemarkt entsteht Risiko nicht mehr nur durch Preisschwankungen, sondern durch das **Profilrisiko** (Kannibalisierungseffekte durch Erneuerbare). Dieses Tool macht sichtbar, wann ein Portfolio "Long" (Überschuss bei niedrigen Preisen) oder "Short" (Mangel bei hohen Preisen) ist.

## 🛠 Technologie-Stack
* **Data Engineering:** Python (Pandas, NumPy, SQLAlchemy)
* **Datenbank:** MySQL (Modulares Schema in 3. Normalform)
* **Reporting:** Automatisierter Excel-Export für P&L und Open Position
* **Tools:** MicroStrategy / Celonis Konzepte für Datenmodellierung genutzt

## 📊 Funktionalitäten
1.  **Simulation:** Generierung von 8760 Stunden (1 Jahr) Marktdaten (Last, Solar, Wind, Preis mit Merit-Order-Effekt).
2.  **Handel:** Abbildung von Base-Load und Peak-Load Futures.
3.  **Analyse:** SQL-gestützter Join von Finanz- und Physikdaten.
4.  **Bewertung:** Stündliche Mark-to-Market (MtM) Bewertung der offenen Position.

## 🚀 Installation & Nutzung
1.  Datenbank-Schema aus `database_schema.sql` in MySQL Workbench ausführen.
2.  Python-Dependencies installieren: `pip install pandas sqlalchemy mysql-connector-python`
3.  Datenbank-Passwort in `config.py` anpassen.
4.  ETL-Prozess starten: `python full_year_simulation.py`
