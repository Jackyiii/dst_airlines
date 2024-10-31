import requests
import pandas as pd

def fetch_city_data(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        city_data = response.json()
        return city_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}. Error: {e}")
        return None


def process_city_data(city_data):
    all_city_info = []
    data_info = city_data['CityResource']['Cities']['City']

    for city in data_info:
        city_code = city.get('CityCode', None)
        country_code = city.get('CountryCode', None)
        utc_offset = city.get('UtcOffset', None)
        time_zone_id = city.get('TimeZoneId', None)

        # 提取城市名
        names = city['Names']['Name']
        if isinstance(names, dict):
            names = [names]

        # 提取机场代码
        airports = city.get('Airports', {})
        airport_codes = []

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

    return all_city_info

def create_city_dataframe(all_city_info):
    return pd.DataFrame(all_city_info)

def filter_cities_with_language(df, language_code):
    cities_with_lang = df.groupby('CountryCode').apply(
        lambda group: language_code in group['LanguageCode'].values
    )
    return cities_with_lang[cities_with_lang].index.tolist()

def process_city_data_workflow(headers, urls):
    """
    Workflow to fetch and process city data from the API.

    Args:
    headers (dict): The headers used for API requests.
    urls (list): The list of URLs for fetching city data.

    Returns:
    pd.DataFrame: DataFrame containing city data filtered by 'EN' language.
    """
    all_city_info = []

    # Fetch and process city data from each URL
    for url in urls:
        city_data = fetch_city_data(url, headers)
        if city_data:
            city_info = process_city_data(city_data)
            all_city_info.extend(city_info)

    # Create DataFrame from all city info
    city_df = create_city_dataframe(all_city_info)
    #selection des colonnes qui nous interresse
    city_df_used=city_df.iloc[:, 0:6]
    print("")
    print('2- données villes')
    # Print the DataFrame
    print(city_df.head())
    print("")
    return city_df
 