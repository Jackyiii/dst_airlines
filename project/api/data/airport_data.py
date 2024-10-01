import requests
import pandas as pd

def fetch_airport_data(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        airport_data = response.json()
        data_info = airport_data['AirportResource']['Airports']['Airport']
        airport_info = []

        for airport in data_info:
            airport_code = airport.get('AirportCode', None)
            city_code = airport.get('CityCode', None)
            country_code = airport.get('CountryCode', None)
            location_type = airport.get('LocationType', None)
            utc_offset = airport.get('UtcOffset', None)
            time_zone_id = airport.get('TimeZoneId', None)

            names = airport['Names']['Name']
            if isinstance(names, dict):
                names = [names]

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
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}. Error: {e}")
        return []

def create_airport_dataframe(all_airport_info):
    return pd.DataFrame(all_airport_info)

def filter_airports_with_language(df, language_code):
    return df[df['LanguageCode'] == language_code]

def process_airport_data_workflow(headers, urls):
    """
    Workflow to fetch and process airport data from the API.

    Args:
    headers (dict): The headers used for API requests.
    urls (list): The list of URLs for fetching airport data.

    Returns:
    pd.DataFrame: DataFrame containing airport data filtered by 'EN' language.
    """
    all_airport_info = []

    # Fetch and process airport data from each URL
    for url in urls:
        airport_info = fetch_airport_data(url, headers)
        all_airport_info.extend(airport_info)

    # Create DataFrame from all airport info
    airport_df = create_airport_dataframe(all_airport_info)

    print(airport_df.head())

    airport_df_en = filter_airports_with_language(airport_df, 'EN')
    print("\nDataFrame with airports having 'EN' as language:\n", airport_df_en)

    return airport_df_en