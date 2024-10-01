import requests
import pandas as pd

def fetch_airline_data(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        airlines_data = response.json()
        data_info = airlines_data['AirlineResource']['Airlines']['Airline']
        airline_info = []

        for airline in data_info:
            airline_id = airline.get('AirlineID', None)
            airline_id_icao = airline.get('AirlineID_ICAO', None)

            names = airline.get('Names', {}).get('Name', None)

            if names:
                if isinstance(names, dict):
                    names = [names]

                for name_entry in names:
                    language_code = name_entry.get('@LanguageCode', None)
                    airline_name = name_entry.get('$', None)
                    airline_info.append({
                        'AirlineID': airline_id,
                        'AirlineID_ICAO': airline_id_icao,
                        'LanguageCode': language_code,
                        'AirlineName': airline_name
                    })
            else:
                airline_info.append({
                    'AirlineID': airline_id,
                    'AirlineID_ICAO': airline_id_icao,
                    'LanguageCode': None,
                    'AirlineName': None
                })

        return airline_info
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}. Error: {e}")
        return []

def create_airline_dataframe(all_airline_info):
    return pd.DataFrame(all_airline_info)

def process_airline_data_workflow(headers, urls):
    """
    Workflow to fetch and process airline data from the API.

    Args:
    headers (dict): The headers used for API requests.
    urls (list): The list of URLs for fetching airline data.

    Returns:
    pd.DataFrame: DataFrame containing the airline data.
    """
    all_airline_info = []

    # Fetch and process airline data from each URL
    for url in urls:
        airline_info = fetch_airline_data(url, headers)
        all_airline_info.extend(airline_info)

    # Create DataFrame from all airline info
    airline_df = create_airline_dataframe(all_airline_info)

    print(airline_df.head())
    print(f"Total rows: {airline_df.shape[0]}")
    print(f"Total columns: {airline_df.shape[1]}")

    return airline_df