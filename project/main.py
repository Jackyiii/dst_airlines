from project.api.data.aircraft_data import process_aircraft_data_workflow
from project.api.data.airline_data import process_airline_data_workflow
from project.api.data.airport_data import process_airport_data_workflow
from project.api.auth import get_access_token
from project.api.data.city_data import process_city_data_workflow
from project.api.data.county_data import process_country_data_workflow
from project.api.data.schedules_data import process_schedules_workflow
from project.config.url import COUNTRY_DATA_URLS, CITY_DATA_URLS, AIRPORT_DATA_URLS, AIRLINE_DATA_URLS, \
    AIRCRAFT_DATA_URLS, DATES, DESTINATIONS, ORIGINS, generate_schedule_urls

from api.data.flight_status import process_flight_status_workflow


def main():
    """
    Main program execution that handles the flow of obtaining an access token and executing subsequent API calls.
    """
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
    country_df_final_lang_en = process_country_data_workflow(headers, COUNTRY_DATA_URLS)

    # get city data
    city_df_final_lang_en = process_city_data_workflow(headers, CITY_DATA_URLS)

    # get airport data
    airport_df_en = process_airport_data_workflow(headers, AIRPORT_DATA_URLS)

    # get airline data
    airline_df = process_airline_data_workflow(headers, AIRLINE_DATA_URLS)

    # get aircraft data
    aircraft_df = process_aircraft_data_workflow(headers, AIRCRAFT_DATA_URLS)
    aircraft_df_fr = aircraft_df.loc[aircraft_df['LanguageCode'] == 'FR']

    # get schedules data
    urls = generate_schedule_urls(ORIGINS, DESTINATIONS, DATES)
    schedules_df, failed_urls = process_schedules_workflow(headers, urls)
    if not schedules_df.empty:
        schedules_df['flightNumber2'] = schedules_df['AirlineID'].astype(str) + schedules_df['FlightNumber'].astype(str)
        print(schedules_df.head())

    if failed_urls:
        print("Les URL suivantes ont échoué :")
        for url in failed_urls:
            print(url)
    # get flight status data
    flights_status_df = process_flight_status_workflow(schedules_df, headers)
    print(flights_status_df.head())

if __name__ == "__main__":
    main()