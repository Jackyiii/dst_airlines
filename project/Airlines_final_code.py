# -*- coding: utf-8 -*-
"""
Created on Mon Aug 12 08:04:15 2024

@author: tahir
"""

import requests
import pandas as pd
import json
# Étape 1: Obtenir un jeton d'accès
url_token = "https://api.lufthansa.com/v1/oauth/token"

# Informations pour l'authentification
client_id = 'keyd3czmh46e4vcf4b5kuyf9u'
client_secret = 'aqPzY9Qvfk'

# Configuration de la requête pour obtenir le token
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
}

data = {
    'client_id': client_id,
    'client_secret': client_secret,
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
#"https://api.lufthansa.com/v1/mds-references/airports/?LHoperated=0",
#   "https://api.lufthansa.com/v1/mds-references/airports/?LHoperated=0&offset=20&limit=20",
#    "https://api.lufthansa.com/v1/mds-references/airports/?LHoperated=0&offset=11840&limit=20"
                                 
                                 

# Liste des URLs des endpoints
urls = [
   "https://api.lufthansa.com/v1/mds-references/airports/?limit=100&offset=0&LHoperated=1&group=AllAirports",
  "https://api.lufthansa.com/v1/mds-references/airports/?limit=100&offset=100&LHoperated=1&group=AllAirports",
  "https://api.lufthansa.com/v1/mds-references/airports/?limit=100&offset=1400&LHoperated=1&group=AllAirports"
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

airport_df_uniq=airport_df['AirportCode'].unique()
airport_df_uniq=pd.DataFrame(airport_df_uniq)
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



                        #Flight Schedules(horaire des vols)




# Liste des URLs pour obtenir les données des horaires des vols
urls = [
    "https://api.lufthansa.com/v1/operations/schedules/FRA/CDG/2024-09-08?directFlights=1&limit=100",
    "https://api.lufthansa.com/v1/operations/schedules/FRA/CDG/2024-09-07?directFlights=1&limit=20&offset=0",
    "https://api.lufthansa.com/v1/operations/schedules/FRA/CDG/2024-09-08?directFlights=1&limit=20&offset=20",
    "https://api.lufthansa.com/v1/operations/schedules/FRA/CDG/2024-09-08T21:40?directFlights=1&limit=20&offset=0"
]

# Configuration de l'en-tête avec le jeton d'accès
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json',
}

# Liste pour stocker toutes les informations des horaires
schedules_info = []

# Fonction pour récupérer et traiter les données depuis une URL
def fetch_schedules_data(url):
    response = requests.get(url, headers=headers)

    # Vérifier si la requête a réussi
    if response.status_code == 200:
        try:
            schedules_data = response.json()
            print(f"Données récupérées avec succès depuis {url}")
        except ValueError:
            print(f"Erreur lors de l'analyse de la réponse JSON pour l'URL: {url}")
            return
    else:
        print("Erreur lors de la récupération des données", "Code de statut:", response.status_code)
        return  # Sortir de la fonction si la requête a échoué

    # Vérifier si 'ScheduleResource' est présent dans les données et est bien un dictionnaire
    if isinstance(schedules_data, dict) and "ScheduleResource" in schedules_data:
        schedule_resource = schedules_data["ScheduleResource"]

        # Vérifier si 'Schedule' est présent et est une liste
        if isinstance(schedule_resource, dict) and "Schedule" in schedule_resource:
            data_info = schedule_resource["Schedule"]

            # Vérifier que 'data_info' est bien une liste
            if isinstance(data_info, list):
                for schedule in data_info:
                    # Vérification supplémentaire pour s'assurer que 'schedule' est bien un dictionnaire
                    if isinstance(schedule, dict):
                        duration = schedule.get('TotalJourney', {}).get('Duration', None)
                        departure_airport_code = schedule.get('Flight', {}).get('Departure', {}).get('AirportCode', None)
                        departure_time = schedule.get('Flight', {}).get('Departure', {}).get('ScheduledTimeLocal', {}).get('DateTime', None)
                        departure_terminal = schedule.get('Flight', {}).get('Departure', {}).get('Terminal', {}).get('Name', None)

                        arrival_airport_code = schedule.get('Flight', {}).get('Arrival', {}).get('AirportCode', None)
                        arrival_time = schedule.get('Flight', {}).get('Arrival', {}).get('ScheduledTimeLocal', {}).get('DateTime', None)
                        arrival_terminal = schedule.get('Flight', {}).get('Arrival', {}).get('Terminal', {}).get('Name', None)

                        airline_id = schedule.get('Flight', {}).get('MarketingCarrier', {}).get('AirlineID', None)
                        flight_number = schedule.get('Flight', {}).get('MarketingCarrier', {}).get('FlightNumber', None)

                        aircraft_code = schedule.get('Flight', {}).get('Equipment', {}).get('AircraftCode', None)
                        stop_quantity = schedule.get('Flight', {}).get('Details', {}).get('Stops', {}).get('StopQuantity', None)
                        days_of_operation = schedule.get('Flight', {}).get('Details', {}).get('DaysOfOperation', None)
                        date_effective = schedule.get('Flight', {}).get('Details', {}).get('DatePeriod', {}).get('Effective', None)
                        date_expiration = schedule.get('Flight', {}).get('Details', {}).get('DatePeriod', {}).get('Expiration', None)

                        # Ajout des informations dans la liste
                        schedules_info.append({
                            'Duration': duration,
                            'AirportCode Dep': departure_airport_code,
                            'DateTime Dep': departure_time,
                            'Terminal Dep': departure_terminal,
                            'AirportCode Arr': arrival_airport_code,
                            'DateTime Arr': arrival_time,
                            'Terminal Arr': arrival_terminal,
                            'AirlineID': airline_id,
                            'FlightNumber': flight_number,
                            'AircraftCode': aircraft_code,
                            'StopQuantity': stop_quantity,
                            'DaysOfOperation': days_of_operation,
                            'Effective': date_effective,
                            'Expiration': date_expiration
                        })
            else:
                print(f"'Schedule' n'est pas une liste dans la réponse pour l'URL: {url}")
        else:
            print(f"'Schedule' non trouvé ou n'est pas un dictionnaire dans la réponse pour l'URL: {url}")
    else:
        print(f"'ScheduleResource' non trouvé ou n'est pas un dictionnaire dans la réponse pour l'URL: {url}")

# Itérer sur chaque URL et récupérer les données
for url in urls:
    fetch_schedules_data(url)

# Créer un DataFrame à partir des informations
schedules_df = pd.DataFrame(schedules_info)

# Afficher le DataFrame
print(schedules_df)
display(schedules_df.head())
print("Nombre total de vols récupérés:", schedules_df.shape[0])

# Créer une nouvelle colonne 'flightNumber2' en concaténant 'AirlineID' et 'FlightNumber'
schedules_df['flightNumber2'] = schedules_df['AirlineID'].astype(str) + schedules_df['FlightNumber'].astype(str)

# Créer un nouveau DataFrame schedules_df2 avec la nouvelle colonne 'flightNumber2'
schedules_df2 = schedules_df.copy()

# Afficher le nouveau DataFrame schedules_df2
print(schedules_df2)
display(schedules_df2.head())


                               #light status 



# Fonction pour extraire la date au format correct depuis 'DateTime Dep'
def extract_date(date_str):
    return date_str.split('T')[0]  # On prend la partie avant le 'T' pour obtenir la date

# Liste pour stocker les informations des statuts de vols
status_flight_info = []

# Itérer sur chaque vol dans schedules_df
for index, row in schedules_df.iterrows():
    flight_number = row['flightNumber2']
    departure_date = extract_date(row['DateTime Dep'])

    # Créer l'URL dynamique en remplaçant le numéro de vol et la date
    flight_status_url = f"https://api.lufthansa.com/v1/operations/flightstatus/{flight_number}/{departure_date}"

    # Effectuer la requête GET pour récupérer les informations sur le statut du vol
    response = requests.get(flight_status_url, headers=headers)

    # Vérifier si la requête a réussi
    if response.status_code == 200:
        try:
            flight_status_data = response.json()
            print(f"Statut récupéré avec succès pour le vol {flight_number} du {departure_date}")
        except ValueError:
            print(f"Erreur lors de l'analyse de la réponse JSON pour le vol {flight_number} du {departure_date}")
            continue
    else:
        print(f"Erreur lors de la récupération des données du statut pour le vol {flight_number} du {departure_date} : Code {response.status_code}")
        continue

    # Extraire les informations de statut pour chaque vol
    data_info = flight_status_data.get("FlightStatusResource", {}).get("Flights", {}).get("Flight", [])
    
    for status in data_info:
        # Informations sur le départ
        departure_airport_code = status.get('Departure', {}).get('AirportCode', None)
        departure_time_local = status.get('Departure', {}).get('ScheduledTimeLocal', {}).get('DateTime', None)
        actual_departure_time_local = status.get('Departure', {}).get('ActualTimeLocal', {}).get('DateTime', None)
        departure_terminal = status.get('Departure', {}).get('Terminal', {}).get('Name', None)
        departure_gate = status.get('Departure', {}).get('Terminal', {}).get('Gate', None)

        # Informations sur l'arrivée
        arrival_airport_code = status.get('Arrival', {}).get('AirportCode', None)
        arrival_time_local = status.get('Arrival', {}).get('ScheduledTimeLocal', {}).get('DateTime', None)
        actual_arrival_time_local = status.get('Arrival', {}).get('ActualTimeLocal', {}).get('DateTime', None)
        arrival_terminal = status.get('Arrival', {}).get('Terminal', {}).get('Name', None)
        arrival_gate = status.get('Arrival', {}).get('Terminal', {}).get('Gate', None)

        # Statut du temps
        time_status_code = status.get('Departure', {}).get('TimeStatus', {}).get('Code', None)
        time_status_definition = status.get('Departure', {}).get('TimeStatus', {}).get('Definition', None)

        # Informations sur la compagnie aérienne
        marketing_airline_id = status.get('MarketingCarrier', {}).get('AirlineID', None)
        marketing_flight_number = status.get('MarketingCarrier', {}).get('FlightNumber', None)

        # Informations sur l'équipement de l'avion
        aircraft_code = status.get('Equipment', {}).get('AircraftCode', None)
        aircraft_registration = status.get('Equipment', {}).get('AircraftRegistration', None)

        # Statut du vol
        flight_status_code = status.get('FlightStatus', {}).get('Code', None)
        flight_status_definition = status.get('FlightStatus', {}).get('Definition', None)

        # Ajouter toutes les informations dans la liste
        status_flight_info.append({
            'FlightNumber': flight_number,
            'Departure Date': departure_date,
            'Departure AirportCode': departure_airport_code,
            'Scheduled Departure Local Time': departure_time_local,
            'Actual Departure Local Time': actual_departure_time_local,
            'Departure Terminal': departure_terminal,
            'Departure Gate': departure_gate,
            'Arrival AirportCode': arrival_airport_code,
            'Scheduled Arrival Local Time': arrival_time_local,
            'Actual Arrival Local Time': actual_arrival_time_local,
            'Arrival Terminal': arrival_terminal,
            'Arrival Gate': arrival_gate,
            'Time Status Code': time_status_code,
            'Time Status Definition': time_status_definition,
            'Marketing Airline ID': marketing_airline_id,
            'Marketing Flight Number': marketing_flight_number,
            'Aircraft Code': aircraft_code,
            'Aircraft Registration': aircraft_registration,
            'Flight Status Code': flight_status_code,
            'Flight Status Definition': flight_status_definition
        })

# Créer un DataFrame à partir des informations de statut des vols
flights_status_df = pd.DataFrame(status_flight_info)

# Afficher le DataFrame
print(flights_status_df)
display(flights_status_df.head())
print(f"Nombre total de vols récupérés: {flights_status_df.shape[0]}")


###################################################################################################################

                    ######Flight Schedules2
import itertools
import requests
import pandas as pd
from datetime import datetime, timedelta

https://api.lufthansa.com/v1/operations/schedules/{origin}/{destination}/{fromDateTime}[?directFlights=1]
# Jeton d'accès (vérifiez qu'il est valide et remplacez-le par votre propre jeton)
access_token = access_token

# Listes des codes d'aéroports pour les pays d'origine et de destination
origins = ['FRA', 'MUC', 'DUS', 'HAM', 'BER']
destinations = ['VIE', 'ZRH', 'JFK', 'ORD', 'LAX', 'PEK', 'NRT', 'JNB']

# Générer la liste des dates (2 jours en arrière et 5 jours en avant à partir d'aujourd'hui)
today = datetime.now()
dates = [(today + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(-2, 6)]

# Configuration de l'en-tête avec le jeton d'accès
headers = {
    'Authorization': f'Bearer {access_token}',  # Assurez-vous que le token est valide
    'Accept': 'application/json',
}

# Liste pour stocker toutes les informations des horaires
schedules_info = []

# Fonction pour récupérer et traiter les données depuis une URL
def fetch_schedules_data(url):
    try:
        response = requests.get(url, headers=headers)

        # Vérifier si la requête a réussi
        if response.status_code == 200:
            try:
                schedules_data = response.json()
                print(f"Données récupérées avec succès depuis {url}")
            except ValueError:
                print(f"Erreur lors de l'analyse de la réponse JSON pour l'URL: {url}")
                return
        else:
            # Gestion de l'erreur 401 (ou autres)
            print(f"Erreur lors de la récupération des données. Code de statut: {response.status_code}")
            return

        # Vérifier si 'ScheduleResource' est présent dans les données et est bien un dictionnaire
        if isinstance(schedules_data, dict) and "ScheduleResource" in schedules_data:
            schedule_resource = schedules_data["ScheduleResource"]

            # Vérifier si 'Schedule' est présent et est une liste
            if isinstance(schedule_resource, dict) and "Schedule" in schedule_resource:
                data_info = schedule_resource["Schedule"]

                # Vérifier que 'data_info' est bien une liste
                if isinstance(data_info, list):
                    for schedule in data_info:
                        if isinstance(schedule, dict):
                            duration = schedule.get('TotalJourney', {}).get('Duration', None)
                            departure_airport_code = schedule.get('Flight', {}).get('Departure', {}).get('AirportCode', None)
                            departure_time = schedule.get('Flight', {}).get('Departure', {}).get('ScheduledTimeLocal', {}).get('DateTime', None)
                            departure_terminal = schedule.get('Flight', {}).get('Departure', {}).get('Terminal', {}).get('Name', None)

                            arrival_airport_code = schedule.get('Flight', {}).get('Arrival', {}).get('AirportCode', None)
                            arrival_time = schedule.get('Flight', {}).get('Arrival', {}).get('ScheduledTimeLocal', {}).get('DateTime', None)
                            arrival_terminal = schedule.get('Flight', {}).get('Arrival', {}).get('Terminal', {}).get('Name', None)

                            airline_id = schedule.get('Flight', {}).get('MarketingCarrier', {}).get('AirlineID', None)
                            flight_number = schedule.get('Flight', {}).get('MarketingCarrier', {}).get('FlightNumber', None)

                            aircraft_code = schedule.get('Flight', {}).get('Equipment', {}).get('AircraftCode', None)
                            stop_quantity = schedule.get('Flight', {}).get('Details', {}).get('Stops', {}).get('StopQuantity', None)
                            days_of_operation = schedule.get('Flight', {}).get('Details', {}).get('DaysOfOperation', None)
                            date_effective = schedule.get('Flight', {}).get('Details', {}).get('DatePeriod', {}).get('Effective', None)
                            date_expiration = schedule.get('Flight', {}).get('Details', {}).get('DatePeriod', {}).get('Expiration', None)

                            # Ajout des informations dans la liste
                            schedules_info.append({
                                'Duration': duration,
                                'AirportCode Dep': departure_airport_code,
                                'DateTime Dep': departure_time,
                                'Terminal Dep': departure_terminal,
                                'AirportCode Arr': arrival_airport_code,
                                'DateTime Arr': arrival_time,
                                'Terminal Arr': arrival_terminal,
                                'AirlineID': airline_id,
                                'FlightNumber': flight_number,
                                'AircraftCode': aircraft_code,
                                'StopQuantity': stop_quantity,
                                'DaysOfOperation': days_of_operation,
                                'Effective': date_effective,
                                'Expiration': date_expiration
                            })
                else:
                    print(f"'Schedule' n'est pas une liste dans la réponse pour l'URL: {url}")
            else:
                print(f"'Schedule' non trouvé ou n'est pas un dictionnaire dans la réponse pour l'URL: {url}")
        else:
            print(f"'ScheduleResource' non trouvé ou n'est pas un dictionnaire dans la réponse pour l'URL: {url}")
    
    except Exception as e:
        print(f"Erreur lors de la requête : {str(e)}")

# Générer les combinaisons origin-destination et origin-origin
combinations = list(itertools.product(origins, destinations)) + [(origin, origin) for origin in origins]

# Itérer sur chaque combinaison (origin, destination) et chaque date
for origin, destination in combinations:
    for date in dates:
        # Générer les 4 URLs dynamiques
        urls = [
            f"https://api.lufthansa.com/v1/operations/schedules/{origin}/{destination}/{date}?directFlights=1&limit=100",
            f"https://api.lufthansa.com/v1/operations/schedules/{origin}/{destination}/{date}?directFlights=1&limit=20&offset=0",
            f"https://api.lufthansa.com/v1/operations/schedules/{origin}/{destination}/{date}?directFlights=1&limit=20&offset=20",
            f"https://api.lufthansa.com/v1/operations/schedules/{origin}/{destination}/{date}?directFlights=1&limit=20&offset=0"
        ]
        
        # Récupérer les données pour chaque URL
        for url in urls:
            fetch_schedules_data(url)

# Créer un DataFrame à partir des informations
if schedules_info:
    schedules_df = pd.DataFrame(schedules_info)

    # Créer une nouvelle colonne 'flightNumber2' en concaténant 'AirlineID' et 'FlightNumber'
    schedules_df['flightNumber2'] = schedules_df['AirlineID'].astype(str) + schedules_df['FlightNumber'].astype(str)

    # Créer un nouveau DataFrame schedules_df2 avec la nouvelle colonne 'flightNumber2'
    schedules_df2 = schedules_df.copy()

    # Afficher le nouveau DataFrame schedules_df2
    print(schedules_df2)
    display(schedules_df2.head())
    print("Nombre total de vols récupérés:", schedules_df.shape[0])
else:
    print("Aucune donnée de vol récupérée. Veuillez vérifier les jetons d'accès ou les paramètres.")



#################
    import time

# Pause de 1 seconde entre les requêtes pour éviter d'atteindre les limites de taux de l'API
time.sleep(1)
#######################
import requests
import pandas as pd
import time

# Fonction pour extraire la date au format correct depuis 'DateTime Dep'
def extract_date(date_str):
    return date_str.split('T')[0]  # On prend la partie avant le 'T' pour obtenir la date

# Liste pour stocker les informations des statuts de vols
status_flight_info = []

# Itérer sur chaque vol dans schedules_df
for index, row in schedules_df.iterrows():
    flight_number = row['flightNumber2']
    departure_date = extract_date(row['DateTime Dep'])

    # Créer l'URL dynamique en remplaçant le numéro de vol et la date
    flight_status_url = f"https://api.lufthansa.com/v1/operations/flightstatus/{flight_number}/{departure_date}"

    # Effectuer la requête GET pour récupérer les informations sur le statut du vol
    response = requests.get(flight_status_url, headers=headers)

    # Vérifier si la requête a réussi
    if response.status_code == 200:
        try:
            flight_status_data = response.json()
            print(f"Statut récupéré avec succès pour le vol {flight_number} du {departure_date}")
        except ValueError:
            print(f"Erreur lors de l'analyse de la réponse JSON pour le vol {flight_number} du {departure_date}")
            continue
    else:
        print(f"Erreur lors de la récupération des données du statut pour le vol {flight_number} du {departure_date} : Code {response.status_code}")
        continue

    # Extraire les informations de statut pour chaque vol
    data_info = flight_status_data.get("FlightStatusResource", {}).get("Flights", {}).get("Flight", [])

    for status in data_info:
        # Informations sur le départ
        departure_airport_code = status.get('Departure', {}).get('AirportCode', None)
        departure_time_local = status.get('Departure', {}).get('ScheduledTimeLocal', {}).get('DateTime', None)
        actual_departure_time_local = status.get('Departure', {}).get('ActualTimeLocal', {}).get('DateTime', None)
        departure_terminal = status.get('Departure', {}).get('Terminal', {}).get('Name', None)
        departure_gate = status.get('Departure', {}).get('Terminal', {}).get('Gate', None)

        # Informations sur l'arrivée
        arrival_airport_code = status.get('Arrival', {}).get('AirportCode', None)
        arrival_time_local = status.get('Arrival', {}).get('ScheduledTimeLocal', {}).get('DateTime', None)
        actual_arrival_time_local = status.get('Arrival', {}).get('ActualTimeLocal', {}).get('DateTime', None)
        arrival_terminal = status.get('Arrival', {}).get('Terminal', {}).get('Name', None)
        arrival_gate = status.get('Arrival', {}).get('Terminal', {}).get('Gate', None)

        # Statut du temps
        time_status_code = status.get('Departure', {}).get('TimeStatus', {}).get('Code', None)
        time_status_definition = status.get('Departure', {}).get('TimeStatus', {}).get('Definition', None)

        # Informations sur la compagnie aérienne
        marketing_airline_id = status.get('MarketingCarrier', {}).get('AirlineID', None)
        marketing_flight_number = status.get('MarketingCarrier', {}).get('FlightNumber', None)

        # Informations sur l'équipement de l'avion
        aircraft_code = status.get('Equipment', {}).get('AircraftCode', None)
        aircraft_registration = status.get('Equipment', {}).get('AircraftRegistration', None)

        # Statut du vol
        flight_status_code = status.get('FlightStatus', {}).get('Code', None)
        flight_status_definition = status.get('FlightStatus', {}).get('Definition', None)

        # Ajouter toutes les informations dans la liste
        status_flight_info.append({
            'FlightNumber': flight_number,
            'Departure Date': departure_date,
            'Departure AirportCode': departure_airport_code,
            'Scheduled Departure Local Time': departure_time_local,
            'Actual Departure Local Time': actual_departure_time_local,
            'Departure Terminal': departure_terminal,
            'Departure Gate': departure_gate,
            'Arrival AirportCode': arrival_airport_code,
            'Scheduled Arrival Local Time': arrival_time_local,
            'Actual Arrival Local Time': actual_arrival_time_local,
            'Arrival Terminal': arrival_terminal,
            'Arrival Gate': arrival_gate,
            'Time Status Code': time_status_code,
            'Time Status Definition': time_status_definition,
            'Marketing Airline ID': marketing_airline_id,
            'Marketing Flight Number': marketing_flight_number,
            'Aircraft Code': aircraft_code,
            'Aircraft Registration': aircraft_registration,
            'Flight Status Code': flight_status_code,
            'Flight Status Definition': flight_status_definition
        })

    # Pause de 1 seconde pour éviter de surcharger l'API
    time.sleep(1)

# Créer un DataFrame à partir des informations de statut des vols
flights_status_df = pd.DataFrame(status_flight_info)

# Afficher le DataFrame
print(flights_status_df)
display(flights_status_df.head())
print(f"Nombre total de vols récupérés: {flights_status_df.shape[0]}")
