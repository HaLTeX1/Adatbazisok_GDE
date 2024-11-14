import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# Beolvassuk a CSV fájlt a pandas könyvtárral
csv_file_path = 'Assets/Hutopanelek.csv'
df = pd.read_csv(csv_file_path, on_bad_lines='skip', delimiter=';')

# Létrehozunk egy SQLite adatbázist és csatlakozunk hozzá
db_file_path = 'Assets/Hutopanelek.db'
conn = sqlite3.connect(db_file_path)

# A DataFrame tartalmát elmentjük egy táblaként az adatbázisban
df.to_sql('hutopanelek', conn, if_exists='replace', index=False)
# Hiányzó értékek ellenőrzése
missing_values = df.isnull().sum()
print("Hiányzó értékek oszloponként:\n", missing_values)

join_query = """
SELECT 
    "Panel hőfok 1 [°C] Time" AS time_1, "Panel hőfok 1 [°C] ValueY" AS value_1,
    "Panel hőfok 2 [°C] Time" AS time_2, "Panel hőfok 2 [°C] ValueY" AS value_2,
    "Panel hőfok 3 [°C] Time" AS time_3, "Panel hőfok 3 [°C] ValueY" AS value_3,
    "Panel hőfok 4 [°C] Time" AS time_4, "Panel hőfok 4 [°C] ValueY" AS value_4,
    "Panel hőfok 5 [°C] Time" AS time_5, "Panel hőfok 5 [°C] ValueY" AS value_5
FROM hutopanelek
"""

joined_data = pd.read_sql(join_query, conn)
print(joined_data, conn)


aggregation_query = """
SELECT 
    AVG("Panel hőfok 1 [°C] ValueY") AS Atlag_1,
    MIN("Panel hőfok 1 [°C] ValueY") AS Min_1,
    MAX("Panel hőfok 1 [°C] ValueY") AS Max_1,

    AVG("Panel hőfok 2 [°C] ValueY") AS Atlag_2,
    MIN("Panel hőfok 2 [°C] ValueY") AS Min_2,
    MAX("Panel hőfok 2 [°C] ValueY") AS Max_2,

    AVG("Panel hőfok 3 [°C] ValueY") AS Atlag_3,
    MIN("Panel hőfok 3 [°C] ValueY") AS Min_3,
    MAX("Panel hőfok 3 [°C] ValueY") AS Max_3,

    AVG("Panel hőfok 4 [°C] ValueY") AS Atlag_4,
    MIN("Panel hőfok 4 [°C] ValueY") AS Min_4,
    MAX("Panel hőfok 4 [°C] ValueY") AS Max_4,

    AVG("Panel hőfok 5 [°C] ValueY") AS Atlag_5,
    MIN("Panel hőfok 5 [°C] ValueY") AS Min_5,
    MAX("Panel hőfok 5 [°C] ValueY") AS Max_5
FROM hutopanelek;
"""

# Végrehajtjuk a lekérdezést és betöltjük az eredményt a DataFrame-be
aggregated_data = pd.read_sql(aggregation_query, conn)

# Az eredmény kiírása
print(aggregated_data)

# Az első lekérdezés futtatása: panel átlaghőmérsékleteinek kiszámítása és tárolása
create_panel_avg_table = """
CREATE TABLE IF NOT EXISTS panel_avg_temperatures AS
SELECT 
    'Panel 1' AS panel, AVG("Panel hőfok 1 [°C] ValueY") AS avg_temperature
FROM hutopanelek
UNION ALL
SELECT 
    'Panel 2', AVG("Panel hőfok 2 [°C] ValueY")
FROM hutopanelek
UNION ALL
SELECT 
    'Panel 3', AVG("Panel hőfok 3 [°C] ValueY")
FROM hutopanelek
UNION ALL
SELECT 
    'Panel 4', AVG("Panel hőfok 4 [°C] ValueY")
FROM hutopanelek
UNION ALL
SELECT 
    'Panel 5', AVG("Panel hőfok 5 [°C] ValueY")
FROM hutopanelek;
"""

# Második lekérdezés: panel átlaghőmérsékletek maximumának kiszámítása és tárolása
create_max_avg_table = """
CREATE TABLE IF NOT EXISTS max_avg_temperature AS
SELECT MAX(avg_temperature) AS max_avg_temperature
FROM panel_avg_temperatures;
"""

# Futtatjuk a lekérdezéseket
conn.execute(create_panel_avg_table)
conn.execute(create_max_avg_table)

# Eredmények lekérdezése és megjelenítése
panel_avg_temperatures = pd.read_sql("SELECT * FROM panel_avg_temperatures", conn)
max_avg_temperature = pd.read_sql("SELECT * FROM max_avg_temperature", conn)

print(panel_avg_temperatures)
print(max_avg_temperature)

try:
    # Tranzakció indítása
    conn.execute('BEGIN TRANSACTION;')

    # Új hőmérsékletmérés hozzáadása a hutopanelek db-hez
    insert_query = """
    INSERT INTO hutopanelek (
        "Panel hőfok 1 [°C] Time", "Panel hőfok 1 [°C] ValueY",
        "Panel hőfok 2 [°C] Time", "Panel hőfok 2 [°C] ValueY",
        "Panel hőfok 3 [°C] Time", "Panel hőfok 3 [°C] ValueY",
        "Panel hőfok 4 [°C] Time", "Panel hőfok 4 [°C] ValueY",
        "Panel hőfok 5 [°C] Time", "Panel hőfok 5 [°C] ValueY"
    ) VALUES (
        '2024-01-01 12:00:00', 22.5,
        '2024-01-01 12:00:00', 23.0,
        '2024-01-01 12:00:00', 21.8,
        '2024-01-01 12:00:00', 20.5,
        '2024-01-01 12:00:00', 24.1
    );
    """
    conn.execute(insert_query)

    # Tranzakció bevitele
    conn.commit()
    print("Az új adat sikeresen hozzáadva a táblához!.")

except Exception as e:
    # Hibakezelés és visszagörgetés
    conn.rollback()
    print("Hiba történt, a tranzakció semlegesítve!:", e)


# Lezárjuk az adatbázis kapcsolatot
conn.close()

print("CSV fájl sikeresen átalakítva SQLite adatbázissá!")
