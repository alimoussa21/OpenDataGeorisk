# import_meteo.py
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# 1. Charge les variables d'environnement
load_dotenv()

# 2. Connexion à PostgreSQL Azure
DB_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}?sslmode=require"
engine = create_engine(DB_URL)

# 3. Charge et transforme les données
def process_data():
    # Charge les fichiers
    df_meteo = pd.read_csv("QUOT_SIM2_latest-20250401-20250504.csv", sep=";")
    df_coords = pd.read_csv("coordonnees_grille_safran_lambert-2-etendu.csv", sep=";")

    # Nettoie les noms de colonnes
    df_meteo = df_meteo.rename(columns={"LAMBX": "x_lambert", "LAMBY": "y_lambert"})
    df_coords = df_coords.rename(columns={"LAMBX (hm)": "x_lambert", "LAMBY (hm)": "y_lambert"})

    # Convertit les coordonnées (hm → m)
    df_coords["x_lambert"] = df_coords["x_lambert"] * 100
    df_coords["y_lambert"] = df_coords["y_lambert"] * 100

    # Jointure et nettoyage
    df_final = pd.merge(
        df_meteo,
        df_coords[["x_lambert", "y_lambert", "LAT_DG", "LON_DG"]],
        on=["x_lambert", "y_lambert"],
        how="left"
    ).dropna(subset=["LAT_DG", "LON_DG"])

    # Standardisation
    df_final["lat"] = df_final["LAT_DG"].str.replace(",", ".").astype(float)
    df_final["lon"] = df_final["LON_DG"].str.replace(",", ".").astype(float)
    df_final["date"] = pd.to_datetime(df_final["DATE"], format="%Y%m%d")

    return df_final[["date", "lat", "lon", "T_Q", "TINF_H_Q", "TSUP_H_Q"]].rename(columns={
        "T_Q": "temp_avg",
        "TINF_H_Q": "temp_min",
        "TSUP_H_Q": "temp_max"
    })

# 4. Vérification avant import
def verify_data(df):
    print("=== Aperçu des données ===")
    print(df.head())
    print("\n=== Statistiques ===")
    print(f"Lignes totales : {len(df)}")
    print(f"Période : {df['date'].min()} → {df['date'].max()}")
    print(f"Latitudes : {df['lat'].min()} → {df['lat'].max()}")
    print(f"Longitudes : {df['lon'].min()} → {df['lon'].max()}")

# 5. Import dans PostgreSQL
def import_to_postgres(df):
    df.to_sql(
        "meteo_data",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000
    )
    print("Import terminé avec succès !")

# Exécution
if __name__ == "__main__":
    data = process_data()
    verify_data(data)
    
    if input("Voulez-vous importer ces données dans PostgreSQL ? (o/n) ").lower() == "o":
        import_to_postgres(data)
