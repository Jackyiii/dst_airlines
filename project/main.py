import os
import tempfile
import requests
import pandas as pd
import pickle
from sqlalchemy import create_engine
from io import StringIO
from config.database import test_connection
from api.auth import get_access_token
from api.data.aircraft_data import process_aircraft_data_workflow
from api.data.airline_data import process_airline_data_workflow
from api.data.airport_data import process_airport_data_workflow
from api.data.city_data import process_city_data_workflow
from api.data.county_data import process_country_data_workflow
from api.data.schedules_data import process_schedules_workflow
from api.data.flight_status import process_flight_status_workflow
from config.url import (
    COUNTRY_DATA_URLS, CITY_DATA_URLS, AIRPORT_DATA_URLS, AIRLINE_DATA_URLS,
    AIRCRAFT_DATA_URLS, DATES, DESTINATIONS, ORIGINS, generate_schedule_urls
)
from config.region_mapping import region_mapping

# Connexion base de données
DATABASE_URL = 'postgresql://myuser:mypassword@db:5432/mydatabase'
engine = create_engine(DATABASE_URL)

# Fichiers temporaires (compatibles Linux/Docker/Windows)
HEADERS_FILE = os.path.join(tempfile.gettempdir(), 'headers.pkl')
DATAFRAMES_FILE = os.path.join(tempfile.gettempdir(), 'dataframes.pkl')

# Étape 1 : Authentification
def download_data():
    print("Téléchargement des données depuis les APIs.")
    test_connection()
    token = get_access_token()
    if not token:
        raise Exception("Échec de l'authentification API.")
    headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
    with open(HEADERS_FILE, 'wb') as f:
        pickle.dump(headers, f)

# Étape 2 : Transformation des données
def transform_data():
    print("Transformation des données.")
    with open(HEADERS_FILE, 'rb') as f:
        headers = pickle.load(f)

    dataframes = {
        'country': process_country_data_workflow(headers, COUNTRY_DATA_URLS),
        'city': process_city_data_workflow(headers, CITY_DATA_URLS),
        'airport': process_airport_data_workflow(headers, AIRPORT_DATA_URLS),
        'airline': process_airline_data_workflow(headers, AIRLINE_DATA_URLS),
        'aircraft': process_aircraft_data_workflow(headers, AIRCRAFT_DATA_URLS),
    }

    schedule_urls = generate_schedule_urls(ORIGINS, DESTINATIONS, DATES)
    schedules_df, failed_urls = process_schedules_workflow(headers, schedule_urls)
    if failed_urls:
        print("URLs échouées :", failed_urls)

    schedules_df['ScheduleID'] = schedules_df['AirlineID'].astype(str) + schedules_df['FlightNumber'].astype(str)
    dataframes['schedules'] = schedules_df
    dataframes['status_flight'] = process_flight_status_workflow(schedules_df, headers)

    with open(DATAFRAMES_FILE, 'wb') as f:
        pickle.dump(dataframes, f)

# Étape 3 : Création de la base (fictive)
def create_database():
    print("Étape de création de la base - gérée automatiquement via SQLAlchemy.")

# Étape 4 : Insertion des données
def insert_data():
    print("Insertion des données dans PostgreSQL.")
    with open(DATAFRAMES_FILE, 'rb') as f:
        dataframes = pickle.load(f)

    for table, df in dataframes.items():
        insert_dataframe_to_sql(df, table)

# Étape 5 : Entraînement modèle ML sur les langues
def split_and_train_language():
    print("Téléchargement et traitement des langues.")
    try:
        df_languages = process_languages_data()
        insert_dataframe_to_sql(df_languages, 'languages')
    except Exception as e:
        print(f"Erreur pendant le traitement des langues : {e}")

# Utilitaire : insertion SQL
def insert_dataframe_to_sql(df: pd.DataFrame, table: str):
    try:
        df.to_sql(table, con=engine, if_exists='append', index=False, chunksize=5000)
        print(f"{len(df)} lignes insérées dans la table {table}")
    except Exception as e:
        print(f"Erreur d'insertion dans {table} : {e}")

# Utilitaire : traitement des langues
def process_languages_data() -> pd.DataFrame:
    url = "https://raw.githubusercontent.com/datasets/language-codes/main/data/language-codes-full.csv"
    response = requests.get(url)
    if response.status_code == 200:
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)
        df = df[df['alpha2'].notna()]
        df.rename(columns={
            'alpha3-b': 'LanguageCode0',
            'alpha3-t': 'LanguageCode1',
            'alpha2': 'LanguageCode',
            'English': 'LanguageName',
            'French': 'LanguageName_fr'
        }, inplace=True)
        df['LanguageCode'] = df['LanguageCode'].str.upper()
        df['Region'] = df['LanguageCode'].map(region_mapping)
        return df[['LanguageCode', 'LanguageName', 'Region']]
    else:
        raise Exception(f"Erreur téléchargement langues : {response.status_code}")

# Point d’entrée
def main():
    download_data()
    transform_data()
    create_database()
    insert_data()
    split_and_train_language()
    print("Pipeline terminé avec succès.")

# Pour Airflow
def run():
    main()

if __name__ == "__main__":
    main()
