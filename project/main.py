
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

def main():
    """
    Main program execution that handles the flow of obtaining an access token and executing subsequent API calls.
    """
    test_connection()
    access_token = get_access_token()

    if access_token:
        print("Access token obtained successfully!")
    else:
        print("Failed to obtain access token. Exiting program.")

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json',
    }

    # get county data
    country_df = process_country_data_workflow(headers, COUNTRY_DATA_URLS)

    # get city data
    city_df = process_city_data_workflow(headers, CITY_DATA_URLS)

    # get airport data
    airport_df = process_airport_data_workflow(headers, AIRPORT_DATA_URLS)

    # get airline data
    airline_df = process_airline_data_workflow(headers, AIRLINE_DATA_URLS)

    # get aircraft data
    aircraft_df = process_aircraft_data_workflow(headers, AIRCRAFT_DATA_URLS)

    # get schedules data
    urls = generate_schedule_urls(ORIGINS, DESTINATIONS, DATES)
    schedules_df, failed_urls = process_schedules_workflow(headers, urls)
    if not schedules_df.empty:
        schedules_df['ScheduleID'] = schedules_df['AirlineID'].astype(str) + schedules_df['FlightNumber'].astype(str)
        print("")
        print("6- données des horaires des vols")
        print(schedules_df.head())
        print("")

    if failed_urls:
        print("Les URL suivantes ont échoué :")
        for url in failed_urls:
            print(url)

    # get flight status data
    flights_status_df = process_flight_status_workflow(schedules_df, headers)
    print("")
    print("7- données de statut des vols")
    print(flights_status_df.head())

    # get language data
    print("")
    print("8- données langues")
    # URL du fichier CSV contenant les codes de langue (ISO 639-1, ISO 639-2, noms de langue)
    url = "https://raw.githubusercontent.com/datasets/language-codes/main/data/language-codes-full.csv"
    # Téléchargement du fichier CSV
    response = requests.get(url)
    # Vérification du statut de la requête
    if response.status_code == 200:
        # Conversion du contenu en un format lisible par Pandas
        csv_data = StringIO(response.text)
        # Chargement des données dans un DataFrame
        languages_df = pd.read_csv(csv_data)
    
        # Filtrage des lignes où 'alpha2' est différent de NaN
        languages_df = languages_df[languages_df['alpha2'].notna()]
        # Renommer les colonnes 'alpha2' et 'English'
        languages_df.rename(columns={
            'alpha3-b': 'LanguageCode0',
            'alpha3-t': 'LanguageCode1',
            'alpha2': 'LanguageCode',
            'English': 'LanguageName',
            'French': 'LanguageName_fr'
        }, inplace=True)

        # Transformer la colonne 'LanguageCode' en majuscules
        languages_df['LanguageCode'] = languages_df['LanguageCode'].str.upper()

        # Dictionnaire de correspondance des codes de langue et des régions
        region_mapping = {
        'AA': 'Afrique de l\'Est',
        'AB': 'Europe de l\'Est',
        'AE': 'Asie centrale',
        'AF': 'Afrique du Sud',
        'AK': 'Afrique de l\'Ouest',
        'AM': 'Afrique de l\'Est',
        'AN': 'Europe (Espagne)',
        'AR': 'Moyen-Orient et Afrique',
        'AS': 'Asie (Inde)',
        'AV': 'Asie (Caucase)',
        'AY': 'Amérique du Sud',
        'AZ': 'Asie (Azerbaïdjan)',
        'BA': 'Europe de l\'Est',
        'BE': 'Europe de l\'Est',
        'BG': 'Europe de l\'Est',
        'BI': 'Asie (Turquie)',
        'BM': 'Afrique de l\'Ouest',
        'BN': 'Asie (Inde, Bangladesh)',
        'BO': 'Asie (Inde)',
        'BR': 'Europe (France)',
        'BS': 'Europe (Bosnie)',
        'CA': 'Europe (Espagne)',
        'CE': 'Asie (Caucase)',
        'CH': 'Océan Pacifique',
        'CO': 'Europe (France)',
        'CR': 'Amérique du Nord',
        'CS': 'Europe (République tchèque)',
        'CU': 'Asie (Russie)',
        'CV': 'Europe (Russie)',
        'CY': 'Europe (Royaume-Uni)',
        'DA': 'Europe (Danemark)',
        'DE': 'Europe (Allemagne)',
        'DV': 'Asie (Maldives)',
        'DZ': 'Asie (Bhoutan)',
        'EE': 'Europe (Estonie)',
        'EL': 'Europe (Grèce)',
        'EN': 'Monde entier',
        'EO': 'Monde entier',
        'ES': 'Monde entier',
        'ET': 'Europe (Estonie)',
        'EU': 'Europe (Espagne)',
        'FA': 'Asie (Iran)',
        'FF': 'Afrique de l\'Ouest',
        'FI': 'Europe (Finlande)',
        'FJ': 'Océan Pacifique',
        'FO': 'Europe (Danemark)',
        'FR': 'Monde entier',
        'FY': 'Europe (Pays-Bas)',
        'GA': 'Europe (Irlande)',
        'GD': 'Europe (Royaume-Uni)',
        'GL': 'Océan Atlantique',
        'GN': 'Afrique de l\'Ouest',
        'GU': 'Asie (Inde)',
        'GV': 'Europe (Royaume-Uni)',
        'HA': 'Afrique de l\'Ouest',
        'HE': 'Moyen-Orient',
        'HI': 'Asie (Inde)',
        'HO': 'Océan Pacifique',
        'HR': 'Europe (Croatie)',
        'HT': 'Amérique (Haïti)',
        'HU': 'Europe (Hongrie)',
        'HY': 'Asie (Arménie)',
        'HZ': 'Afrique (Namibie)',
        'IA': 'Monde entier',
        'ID': 'Asie (Indonésie)',
        'IE': 'Europe (Irlande)',
        'IG': 'Afrique (Nigeria)',
        'II': 'Asie (Chine)',
        'IK': 'Amérique du Nord',
        'IO': 'Monde entier',
        'IS': 'Europe (Islande)',
        'IT': 'Europe (Italie)',
        'IU': 'Amérique du Nord',
        'JA': 'Asie (Japon)',
        'JV': 'Asie (Indonésie)',
        'KA': 'Europe (Géorgie)',
        'KG': 'Asie (Kirghizistan)',
        'KI': 'Océan Pacifique',
        'KJ': 'Afrique (Namibie)',
        'KK': 'Asie (Kazakhstan)',
        'KL': 'Océan Atlantique',
        'KM': 'Océan Indien',
        'KN': 'Asie (Inde)',
        'KO': 'Asie (Corée)',
        'KR': 'Asie (Russie)',
        'KS': 'Asie (Inde)',
        'KU': 'Moyen-Orient',
        'KV': 'Europe (Russie)',
        'KW': 'Moyen-Orient',
        'KY': 'Amérique (Cayenne)',
        'LA': 'Monde entier',
        'LB': 'Europe (Luxembourg)',
        'LG': 'Afrique (Ouganda)',
        'LI': 'Europe (Italie)',
        'LN': 'Afrique (République Démocratique du Congo)',
        'LO': 'Asie (Laos)',
        'LT': 'Europe (Lituanie)',
        'LU': 'Europe (Luxembourg)',
        'LV': 'Europe (Lettonie)',
        'MG': 'Afrique (Madagascar)',
        'MH': 'Océan Pacifique',
        'MI': 'Océan Pacifique',
        'MK': 'Europe (Macédoine)',
        'ML': 'Asie (Inde)',
        'MN': 'Asie (Mongolie)',
        'MR': 'Asie (Inde)',
        'MS': 'Asie (Malaisie)',
        'MT': 'Europe (Malte)',
        'MY': 'Asie (Myanmar)',
        'NA': 'Océan Pacifique',
        'NB': 'Europe (Norvège)',
        'ND': 'Afrique (Zimbabwe)',
        'NE': 'Europe (Pays-Bas)',
        'NG': 'Afrique (Nigeria)',
        'NL': 'Europe (Pays-Bas)',
        'NN': 'Europe (Norvège)',
        'NO': 'Europe (Norvège)',
        'NR': 'Afrique (Zimbabwe)',
        'NV': 'Amérique du Nord',
        'NY': 'Afrique (Malawi)',
        'OC': 'Europe (France)',
        'OJ': 'Amérique du Nord',
        'OM': 'Afrique (Éthiopie)',
        'OR': 'Asie (Inde)',
        'OS': 'Europe (Russie)',
        'PA': 'Asie (Inde, Pakistan)',
        'PI': 'Asie (Inde)',
        'PL': 'Europe (Pologne)',
        'PS': 'Asie (Afghanistan, Pakistan)',
        'PT': 'Monde entier',
        'QU': 'Amérique du Sud',
        'RM': 'Europe (Suisse)',
        'RN': 'Afrique (Burundi)',
        'RO': 'Europe (Roumanie)',
        'RU': 'Europe (Russie)',
        'RW': 'Afrique (Rwanda)',
        'SA': 'Asie (Inde)',
        'SC': 'Océan Indien',
        'SD': 'Asie (Inde, Pakistan)',
        'SE': 'Europe (Suède)',
        'SG': 'Afrique (République Centrafricaine)',
        'SI': 'Asie (Sri Lanka)',
        'SK': 'Europe (Slovaquie)',
        'SL': 'Afrique (Lesotho)',
        'SM': 'Océan Pacifique',
        'SN': 'Afrique (Sénégal)',
        'SO': 'Afrique de l\'Est',
        'SQ': 'Europe (Albanie)',
        'SR': 'Europe (Serbie)',
        'SS': 'Afrique (Eswatini)',
        'ST': 'Afrique (Lesotho)',
        'SU': 'Asie (Indonésie)',
        'SV': 'Europe (Suède)',
        'SW': 'Afrique de l\'Est',
        'TA': 'Asie (Inde, Sri Lanka)',
        'TE': 'Asie (Inde)',
        'TG': 'Afrique (Éthiopie, Érythrée)',
        'TH': 'Asie (Thaïlande)',
        'TI': 'Afrique (Éthiopie, Érythrée)',
        'TK': 'Océan Pacifique',
        'TL': 'Asie (Timor oriental)',
        'TN': 'Afrique du Nord',
        'TO': 'Océan Pacifique',
        'TR': 'Europe et Asie',
        'TS': 'Afrique (Afrique du Sud)',
        'TT': 'Europe (Russie)',
        'TW': 'Afrique (Ghana)',
        'TY': 'Océan Pacifique',
        'UG': 'Afrique (Ouganda)',
        'UK': 'Europe (Ukraine)',
        'UR': 'Asie (Inde, Pakistan)',
        'UZ': 'Asie (Ouzbékistan)',
        'VE': 'Afrique (Afrique du Sud)',
        'VI': 'Asie (Vietnam)',
        'VO': 'Monde entier',
        'WA': 'Europe (Belgique)',
        'WO': 'Afrique (Sénégal)',
        'XH': 'Afrique (Afrique du Sud)',
        'YI': 'Europe (Juif)',
        'YO': 'Afrique (Nigeria)',
        'ZA': 'Asie (Turquie)',
        'ZH': 'Asie (Chine)',
        'ZU': 'Afrique (Afrique du Sud)'
        }

        # Ajout de la colonne 'Region' en utilisant le dictionnaire de correspondance
        languages_df['Region'] = languages_df['LanguageCode'].map(region_mapping)
        # selection des variables
        languages_df = languages_df[['LanguageCode', 'LanguageName', 'Region']]

        # Affichage du DataFrame avec la nouvelle colonne
        print(languages_df.head())

        # Affichage de la forme du DataFrame et des premières lignes pour vérification
        print(languages_df.shape)
        #Dictionnaire associant les DataFrames aux tables PostgreSQL
        dataframes_to_tables = {
            'country': country_df,
            'city': city_df,
            'airport': airport_df,
            'airline': airline_df,
            'aircraft': aircraft_df,
            'schedules': schedules_df,
            'status_flight': flights_status_df,
            'languages': languages_df
            }
        # Insertion de tous les DataFrames dans leurs tables respectives
        for table_name, dataframe in dataframes_to_tables.items():
            insert_dataframe_to_sql(dataframe, table_name, engine)
        print("finish execution for DataFrame insertion")
    else:
        print("Erreur lors du téléchargement du fichier CSV :", response.status_code)


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
        dataframe.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Inserted {len(dataframe)} rows into {table_name}")
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")


if __name__ == "__main__":
    main()

