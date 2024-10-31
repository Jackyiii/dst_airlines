import requests
import pandas as pd

def fetch_aircraft_data(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        aircraft_data = response.json()
        data_info = aircraft_data["AircraftResource"]['AircraftSummaries']['AircraftSummary']
        aircraft_info = []

        for aircraft in data_info:
            aircraft_code = aircraft.get('AircraftCode', None)
            airline_equip_code = aircraft.get('AirlineEquipCode', None)

            names = aircraft.get('Names', {}).get('Name', None)

            if names:
                if isinstance(names, dict):
                    names = [names]

                for name_entry in names:
                    language_code = name_entry.get('@LanguageCode', None)
                    aircraft_name = name_entry.get('$', None)
                    aircraft_info.append({
                        'AircraftCode': aircraft_code,
                        'LanguageCode': language_code,
                        'AircraftName': aircraft_name,
                        'AirlineEquipCode': airline_equip_code
                    })
            else:
                aircraft_info.append({
                    'AircraftCode': aircraft_code,
                    'LanguageCode': None,
                    'AircraftName': None,
                    'AirlineEquipCode': airline_equip_code
                })

        return aircraft_info
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}. Error: {e}")
        return []

def create_aircraft_dataframe(all_aircraft_info):
    return pd.DataFrame(all_aircraft_info)

def process_aircraft_data_workflow(headers, urls):
    """
    Workflow to fetch and process aircraft data from the API.

    Args:
    headers (dict): The headers used for API requests.
    urls (list): The list of URLs for fetching aircraft data.

    Returns:
    pd.DataFrame: DataFrame containing the aircraft data.
    """
    all_aircraft_info = []

    # Fetch and process aircraft data from each URL
    for url in urls:
        aircraft_info = fetch_aircraft_data(url, headers)
        all_aircraft_info.extend(aircraft_info)

    # Create DataFrame from all aircraft info
    aircraft_df = create_aircraft_dataframe(all_aircraft_info)

    # 打印 DataFrame
    print("")
    print("5- données des aeronefs")
    print(aircraft_df.head())
    #print(f"Total rows: {aircraft_df.shape[0]}")
    #print(f"Total columns: {aircraft_df.shape[1]}")
    print("")

    return aircraft_df