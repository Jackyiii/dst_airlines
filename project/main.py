import matplotlib
import pandas as pd
import requests
import os
from sqlalchemy import create_engine
import psycopg2
from io import StringIO
from api.data.aircraft_data import process_aircraft_data_workflow
from api.data.airline_data import process_airline_data_workflow
from api.data.airport_data import process_airport_data_workflow
from api.auth import get_access_token
from api.data.city_data import process_city_data_workflow
from api.data.county_data import process_country_data_workflow
from api.data.schedules_data import process_schedules_workflow
from config.url import COUNTRY_DATA_URLS, CITY_DATA_URLS, AIRPORT_DATA_URLS, AIRLINE_DATA_URLS, \
    AIRCRAFT_DATA_URLS, DATES, DESTINATIONS, ORIGINS, generate_schedule_urls
from api.data.flight_status import process_flight_status_workflow
from config.database import get_connection, test_connection
from config.region_mapping import region_mapping

def process_all_data(headers):
    """
    Process all data workflows and return a dictionary of DataFrames.
    """
    dataframes = {
        'country': process_country_data_workflow(headers, COUNTRY_DATA_URLS),
        'city': process_city_data_workflow(headers, CITY_DATA_URLS),
        'airport': process_airport_data_workflow(headers, AIRPORT_DATA_URLS),
        'airline': process_airline_data_workflow(headers, AIRLINE_DATA_URLS),
        'aircraft': process_aircraft_data_workflow(headers, AIRCRAFT_DATA_URLS),
    }
    # Process schedules data
    urls = generate_schedule_urls(ORIGINS, DESTINATIONS, DATES)
    schedules_df, failed_urls = process_schedules_workflow(headers, urls)
    if failed_urls:
        print("Les URL suivantes ont échoué :", failed_urls)
    schedules_df['ScheduleID'] = schedules_df['AirlineID'].astype(str) + schedules_df['FlightNumber'].astype(str)
    dataframes['schedules'] = schedules_df

    # Process flight status data
    dataframes['status_flight'] = process_flight_status_workflow(schedules_df, headers)
    return dataframes

def process_languages_data() -> pd.DataFrame:
    """
    Download and process language data.
    """
    url = "https://raw.githubusercontent.com/datasets/language-codes/main/data/language-codes-full.csv"
    response = requests.get(url)
    if response.status_code == 200:
        csv_data = StringIO(response.text)
        languages_df = pd.read_csv(csv_data)
        languages_df = languages_df[languages_df['alpha2'].notna()]
        languages_df.rename(columns={
            'alpha3-b': 'LanguageCode0',
            'alpha3-t': 'LanguageCode1',
            'alpha2': 'LanguageCode',
            'English': 'LanguageName',
            'French': 'LanguageName_fr'
        }, inplace=True)
        languages_df['LanguageCode'] = languages_df['LanguageCode'].str.upper()
        languages_df['Region'] = languages_df['LanguageCode'].map(region_mapping)
        return languages_df[['LanguageCode', 'LanguageName', 'Region']]
    else:
        raise Exception(f"Failed to download language data: {response.status_code}")


def main():
    test_connection()
    access_token = get_access_token()

    if not access_token:
        print("Failed to obtain access token. Exiting program.")
        return

    headers = {'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'}
    dataframes = process_all_data(headers)
    try:
        dataframes['languages'] = process_languages_data()
    except Exception as e:
        print(e)

    for table_name, dataframe in dataframes.items():
        insert_dataframe_to_sql(dataframe, table_name, engine)
    print("finish execution for DataFrame insertion")


# Configuration des paramètres de la base de données
#DATABASE_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@db:5432/{os.getenv('POSTGRES_DB')}"
DATABASE_URL  = 'postgresql://myuser:mypassword@db:5432/mydatabase'
print(DATABASE_URL)
# Créer une connexion SQLAlchemy
engine = create_engine(DATABASE_URL)

# Fonction générique pour insérer un DataFrame dans une table PostgreSQL
def insert_dataframe_to_sql(dataframe, table_name, engine):
    """
    Insère un DataFrame dans une table PostgreSQL.
    
    Args:
        dataframe (pd.DataFrame): Le DataFrame à insérer.
        table_name (str): Le nom de la table cible.
        engine: L'instance SQLAlchemy Engine pour la connexion.
    """
    try:
        # Insérer les données
        dataframe.to_sql(table_name, con=engine, if_exists='append', index=False,chunksize=5000)
        print(f"Inserted {len(dataframe)} rows into {table_name}")
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")


if __name__ == "__main__":
    main()

