# Base URL
import itertools
from datetime import datetime, timedelta

BASE_URL = "https://api.lufthansa.com/v1"

# Endpoints
TOKEN_URL = f"{BASE_URL}/oauth/token"
FLIGHT_STATUS_URL = f"{BASE_URL}/operations/flightstatus"
AIRPORT_INFO_URL = f"{BASE_URL}/reference-data/airports"

# URLs for country data
COUNTRY_DATA_URLS = [
    f"{BASE_URL}/mds-references/countries/?limit=100&offset=0",
    f"{BASE_URL}/mds-references/countries/?limit=100&offset=100",
    f"{BASE_URL}/mds-references/countries/?limit=100&offset=200"
]

# URLs for country city data
CITY_DATA_URLS = [
    f"{BASE_URL}/mds-references/cities/?limit=100&offset=0",
    f"{BASE_URL}/mds-references/cities/?limit=100&offset=100",
    f"{BASE_URL}/mds-references/cities/?limit=100&offset=200"
]

# URLs for airport data
AIRPORT_DATA_URLS = [
    f"{BASE_URL}/mds-references/airports/?limit=100&offset=0&LHoperated=1&group=AllAirports",
    f"{BASE_URL}/mds-references/airports/?limit=100&offset=100&LHoperated=1&group=AllAirports",
    f"{BASE_URL}/mds-references/airports/?limit=100&offset=1400&LHoperated=1&group=AllAirports"
]

# URLs for airline data
AIRLINE_DATA_URLS = [
    f"{BASE_URL}/mds-references/airlines/?limit=100&offset=0",
    f"{BASE_URL}/mds-references/airlines/?limit=100&offset=100",
    f"{BASE_URL}/mds-references/airlines/?limit=100&offset=1100"
]

# URLs for aircraft data
AIRCRAFT_DATA_URLS = [
    f"{BASE_URL}/mds-references/aircraft/?limit=100&offset=0",
    f"{BASE_URL}/mds-references/aircraft/?limit=100&offset=100",
    f"{BASE_URL}/mds-references/aircraft/?limit=100&offset=300"
]

# Listes des codes d'aéroports pour les pays d'origine et de destination
ORIGINS = ['FRA', 'MUC']
DESTINATIONS = ['ZRH', 'JFK',  'PEK'  ]
# Générer la liste des dates (2 jours en arrière et 5 jours en avant à partir d'aujourd'hui)
TODAY = datetime.now()
DATES = [(TODAY + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(-2, 6)]

def generate_schedule_urls(origins, destinations, dates):
    combinations = list(itertools.product(origins, destinations))
    filtered_combinations = [(origin, destination) for origin, destination in combinations if origin != destination]

    urls = []
    for origin, destination in filtered_combinations:
        for date in dates:
            urls.append(f"{BASE_URL}/operations/schedules/{origin}/{destination}/{date}?directFlights=1&limit=100")
    return urls
