import requests
import pandas as pd
import json
import configuration.authentificationConfig as authConfig

# Étape 1: Obtenir un jeton d'accès
url_token = "https://api.lufthansa.com/v1/oauth/token"

# Configuration de la requête pour obtenir le token
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
}

data = {
    'client_id': authConfig.client_id,
    'client_secret': authConfig.client_secret,
    'grant_type': 'client_credentials',
}

response = requests.post(url_token, headers=headers, data=data)
print(response.json())
dir(response)
# Vérifier si la requête a réussi
if response.status_code == 200:
    access_token = response.json().get('access_token')
    print(f"Jeton d'accès: {access_token}")
else:
    print("Erreur lors de la récupération du jeton d'accès")
    exit()

# Étape 2: Faire une requête GET pour obtenir des données de vol


                                 #1-données pays


# Jeton d'accès (obtenu précédemment)
access_token = access_token

# Liste des URLs des endpoints
urls = [
    "https://api.lufthansa.com/v1/mds-references/countries/?limit=100&offset=0",
    "https://api.lufthansa.com/v1/mds-references/countries/?limit=100&offset=100",
    "https://api.lufthansa.com/v1/mds-references/countries/?limit=100&offset=200"
]

# Configuration de l'en-tête avec le jeton d'accès
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json',
}

# Initialisation d'une liste pour stocker toutes les données des pays
all_country_info = []

# Fonction pour effectuer une requête GET et traiter les données
def fetch_country_data(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        country_data = response.json()
        data_info = country_data['CountryResource']['Countries']['Country']
        # Extraire les informations pour chaque pays
        for country in data_info:
            country_code = country['CountryCode']
            names = country['Names']['Name']
            # Vérifier si `names` est une liste
            if isinstance(names, list):
                for name_entry in names:
                    language_code = name_entry['@LanguageCode']
                    country_name = name_entry['$']
                    all_country_info.append({
                        'CountryCode': country_code,
                        'LanguageCode': language_code,
                        'CountryName': country_name
                    })
            else:
                # Si `names` n'est pas une liste, traiter comme un seul élément
                language_code = names['@LanguageCode']
                country_name = names['$']
                all_country_info.append({
                    'CountryCode': country_code,
                    'LanguageCode': language_code,
                    'CountryName': country_name
                })
    else:
        print(f"Erreur lors de la récupération des données du vol pour {url}. Code statut: {response.status_code}")

# Itérer sur chaque URL et récupérer les données
for url in urls:
    fetch_country_data(url)

# Créer un DataFrame à partir des informations
country_df = pd.DataFrame(all_country_info)

# Afficher le DataFrame
print(country_df)

# Vérifier les dimensions du DataFrame
print(f"Nombre total de lignes: {country_df.shape[0]}")
print(f"Nombre total de colonnes: {country_df.shape[1]}")

# verifier le languageCode le plus frequent en fonction des pays
print(country_df.groupby(country_df['CountryCode']).count())
#a=country_df.groupby(country_df['CountryCode']).count()
#b=country_df.loc[country_df['CountryCode'] == 'KP']

#Verifions si pour chaqque countryCode on n'a "EN" comme langue en commune avec d'autres pays

# Grouper par CountryCode et vérifier la présence de 'EN'
countries_with_en = country_df.groupby('CountryCode').apply(
    lambda group: 'EN' in group['LanguageCode'].values
)

# Filtrer les pays ayant 'EN' comme langue
countries_with_en = countries_with_en[countries_with_en].index.tolist()

# Afficher les CountryCodes ayant au moins 'EN' comme langue
print("Countries with 'EN' as a language:", countries_with_en)

# Optionnel: filtrer le DataFrame original pour afficher seulement ces pays
df_with_en_language = country_df[country_df['CountryCode'].isin(countries_with_en)]

# Afficher le DataFrame filtré
print("\nDataFrame with countries having 'EN' as a language:\n", df_with_en_language)
 #prendre que les ligne ayant un languageCode="EN"
country_df_final_lang_en=df_with_en_language.loc[df_with_en_language['LanguageCode']=='EN']
display(country_df_final_lang_en.head())



                                   # données villes


# Jeton d'accès (obtenu précédemment)
#access_token = 'nbav65rex3h56dkz6bhykqve'

# Liste des URLs des endpoints
urls = [
    "https://api.lufthansa.com/v1/mds-references/cities/?limit=100&offset=0",
    "https://api.lufthansa.com/v1/mds-references/cities/?limit=100&offset=100",
    "https://api.lufthansa.com/v1/mds-references/cities/?limit=100&offset=200"
]

# Configuration de l'en-tête avec le jeton d'accès
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json',
}

# Initialisation d'une liste pour stocker toutes les données des villes
all_city_info = []

# Fonction pour effectuer une requête GET et traiter les données
def fetch_city_data(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        city_data = response.json()
        data_info = city_data['CityResource']['Cities']['City']

        # Extraire les informations pour chaque ville
        for city in data_info:
            city_code = city.get('CityCode', None)
            country_code = city.get('CountryCode', None)
            utc_offset = city.get('UtcOffset', None)
            time_zone_id = city.get('TimeZoneId', None)

            # Extraire les noms dans différentes langues
            names = city['Names']['Name']

            # Vérifier si 'names' est une liste ou un dictionnaire
            if isinstance(names, dict):
                names = [names]

            # Extraire les informations d'aéroport
            airports = city.get('Airports', {})
            airport_codes = []

            # Vérifier si 'airports' est un dictionnaire ou une liste
            if isinstance(airports, dict):
                airport_codes.append(airports.get('AirportCode', None))
            elif isinstance(airports, list):
                airport_codes = [airport.get('AirportCode', None) for airport in airports]

            for name_entry in names:
                language_code = name_entry.get('@LanguageCode', None)
                city_name = name_entry.get('$', None)

                for airport_code in airport_codes:
                    all_city_info.append({
                        'CountryCode': country_code,
                        'CityCode': city_code,
                        'LanguageCode': language_code,
                        'CityName': city_name,
                        'UtcOffset': utc_offset,
                        'TimeZoneId': time_zone_id,
                        'AirportCode': airport_code
                    })

# Itérer sur chaque URL et récupérer les données
for url in urls:
    fetch_city_data(url)

# Créer un DataFrame à partir des informations
city_df = pd.DataFrame(all_city_info)

# Afficher le DataFrame
print(city_df.head())


# Vérifier les dimensions du DataFrame
print(f"Nombre total de lignes: {city_df.shape[0]}")
print(f"Nombre total de colonnes: {city_df.shape[1]}")

# verifier le languageCode le plus frequent en fonction des pays
print(city_df.groupby(city_df['LanguageCode']).count())
#a=city_df.groupby(city_df['CountryCode']).count()
#b=city_df.loc[city_df['CountryCode'] == 'AD']

#Verifions si pour chaqque countryCode on n'a "EN" comme langue en commune avec d'autres pays

# Grouper par CountryCode et vérifier la présence de 'EN'
cities_with_en = city_df.groupby('CountryCode').apply(
    lambda group: 'EN' in group['LanguageCode'].values
)

# Filtrer les pays ayant 'EN' comme langue
cities_with_en = cities_with_en[cities_with_en].index.tolist()

# Afficher les CountryCodes ayant au moins 'EN' comme langue
print("Countries with 'EN' as a language:", cities_with_en)

# Optionnel: filtrer le DataFrame original pour afficher seulement ces pays
df_with_en_language = city_df[city_df['CountryCode'].isin(cities_with_en)]

# Afficher le DataFrame filtré
print("\nDataFrame with countries having 'EN' as a language:\n", df_with_en_language)
 #prendre que les ligne ayant un languageCode="EN"
city_df_final_lang_en=df_with_en_language.loc[df_with_en_language['LanguageCode']=='EN']
display(city_df_final_lang_en.head())



                                 #données Aéroports

# Liste des URLs des endpoints
urls = [
    "https://api.lufthansa.com/v1/mds-references/airports/?LHoperated=1",
    "https://api.lufthansa.com/v1/mds-references/airports/?LHoperated=0&offset=20&limit=20",
    "https://api.lufthansa.com/v1/mds-references/airports/?LHoperated=0&offset=11840&limit=20"
]

# Configuration de l'en-tête avec le jeton d'accès
headers = {
    'Authorization': f'Bearer {access_token}',  # Remplacez par votre jeton d'accès
    'Accept': 'application/json',
}

# Fonction pour effectuer une requête GET et traiter les données
def fetch_airport_data(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        airport_data = response.json()
        data_info = airport_data['AirportResource']['Airports']['Airport']
        airport_info = []

        # Extraire les informations pour chaque aéroport
        for airport in data_info:
            airport_code = airport.get('AirportCode', None)
            city_code = airport.get('CityCode', None)
            country_code = airport.get('CountryCode', None)
            location_type = airport.get('LocationType', None)
            utc_offset = airport.get('UtcOffset', None)
            time_zone_id = airport.get('TimeZoneId', None)

            # Extraire les noms dans différentes langues
            names = airport['Names']['Name']

            # Vérifier si 'names' est une liste ou un dictionnaire
            if isinstance(names, dict):
                names = [names]

            # Extraire les informations des coordonnées
            position = airport['Position']['Coordinate']
            latitude = position.get('Latitude', None)
            longitude = position.get('Longitude', None)

            for name_entry in names:
                language_code = name_entry.get('@LanguageCode', None)
                airport_name = name_entry.get('$', None)

                airport_info.append({
                    'AirportCode': airport_code,
                    'Latitude': latitude,
                    'Longitude': longitude,
                    'CityCode': city_code,
                    'CountryCode': country_code,
                    'LocationType': location_type,
                    'LanguageCode': language_code,
                    'AirportName': airport_name,
                    'UtcOffset': utc_offset,
                    'TimeZoneId': time_zone_id
                })
        return airport_info
    else:
        print("Erreur lors de la récupération des données des aéroports", "Code de statut:", response.status_code)
        return []

# Initialiser une liste pour stocker toutes les données des aéroports
all_airport_info = []

# Itérer sur chaque URL et récupérer les données
for url in urls:
    all_airport_info.extend(fetch_airport_data(url))

# Créer un DataFrame à partir des informations
airport_df = pd.DataFrame(all_airport_info)

# Afficher le DataFrame
print(airport_df)
print(airport_df.groupby(airport_df['LanguageCode']).count())

#afficher les données avec un codelangagene anglais
airport_df_en=airport_df.loc[airport_df['LanguageCode']=='EN']





                        #données compagnies aeriennes



# URLs pour obtenir les données des Compagnies aériennes
urls = [
    "https://api.lufthansa.com/v1/mds-references/airlines/?limit=100&offset=0",
    "https://api.lufthansa.com/v1/mds-references/airlines/?limit=100&offset=100",
    "https://api.lufthansa.com/v1/mds-references/airlines/?limit=100&offset=1100"
]

# Configuration de l'en-tête avec le jeton d'accès
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json',
}

# Initialisation d'une liste pour stocker toutes les informations des compagnies aériennes
all_airline_info = []

# Fonction pour récupérer et traiter les données depuis une URL
def fetch_airline_data(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        airlines_data = response.json()
        data_info = airlines_data['AirlineResource']['Airlines']['Airline']

        for airline in data_info:
            Airline_ID = airline.get('AirlineID', None)
            Airline_ID_ICAO = airline.get('AirlineID_ICAO', None)

            # Extraire les noms dans différentes langues
            names = airline.get('Names', {}).get('Name', None)

            if names:
                # Vérifier si 'names' est une liste ou un dictionnaire
                if isinstance(names, dict):
                    # Si c'est un dictionnaire, le traiter comme un seul élément
                    names = [names]

                for name_entry in names:
                    language_code = name_entry.get('@LanguageCode', None)
                    airline_name = name_entry.get('$', None)
                    all_airline_info.append({
                        'AirlineID': Airline_ID,
                        'AirlineID_ICAO': Airline_ID_ICAO,
                        'LanguageCode': language_code,
                        'AirlineName': airline_name
                    })
            else:
                # Si 'Names' ou 'Name' est manquant, ajouter l'entrée avec des valeurs None
                all_airline_info.append({
                    'AirlineID': Airline_ID,
                    'AirlineID_ICAO': Airline_ID_ICAO,
                    'LanguageCode': None,
                    'AirlineName': None
                })
    else:
        print("Erreur lors de la récupération des données des compagnies", "Code de statut:", response.status_code)

# Itérer sur chaque URL et récupérer les données
for url in urls:
    fetch_airline_data(url)

# Créer un DataFrame à partir des informations
airline_df = pd.DataFrame(all_airline_info)

# Afficher le DataFrame
print(airline_df)
print(airline_df.shape)



                    #données des aeronefs


# URLs pour obtenir les données des Aéronefs
urls = [
    "https://api.lufthansa.com/v1/mds-references/aircraft/?limit=100&offset=0",
    "https://api.lufthansa.com/v1/mds-references/aircraft/?limit=100&offset=100",
    "https://api.lufthansa.com/v1/mds-references/aircraft/?limit=100&offset=300"
]

# Configuration de l'en-tête avec le jeton d'accès
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json',
}

# Initialisation d'une liste pour stocker toutes les informations des aéronefs
all_aircraft_info = []

# Fonction pour récupérer et traiter les données depuis une URL
def fetch_aircraft_data(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        aircraft_data = response.json()
        data_info = aircraft_data["AircraftResource"]['AircraftSummaries']['AircraftSummary']

        for aircraft in data_info:
            aircraft_code = aircraft.get('AircraftCode', None)
            airline_equip_code = aircraft.get('AirlineEquipCode', None)

            # Extraire les noms dans différentes langues
            names = aircraft.get('Names', {}).get('Name', None)

            if names:
                # Vérifier si 'names' est une liste ou un dictionnaire
                if isinstance(names, dict):
                    # Si c'est un dictionnaire, le traiter comme un seul élément
                    names = [names]

                for name_entry in names:
                    language_code = name_entry.get('@LanguageCode', None)
                    aircraft_name = name_entry.get('$', None)
                    all_aircraft_info.append({
                        'AircraftCode': aircraft_code,
                        'LanguageCode': language_code,
                        'AircraftName': aircraft_name,
                        'AirlineEquipCode': airline_equip_code
                    })
            else:
                # Si 'Names' ou 'Name' est manquant, ajouter l'entrée avec des valeurs None
                all_aircraft_info.append({
                    'AircraftCode': aircraft_code,
                    'LanguageCode': None,
                    'AircraftName': None,
                    'AirlineEquipCode': airline_equip_code
                })
    else:
        print("Erreur lors de la récupération des données des aéronefs", "Code de statut:", response.status_code)

# Itérer sur chaque URL et récupérer les données
for url in urls:
    fetch_aircraft_data(url)

# Créer un DataFrame à partir des informations
aircraft_df = pd.DataFrame(all_aircraft_info)

# Afficher le DataFrame
print(aircraft_df.head())
display(aircraft_df.head())

# Afficher la forme du DataFrame
print(aircraft_df.shape)

# Afficher les données avec un code langue spécifique (par exemple, 'FR' pour le français)
aircraft_df_fr = aircraft_df.loc[aircraft_df['LanguageCode'] == 'FR']
print(aircraft_df_fr)
