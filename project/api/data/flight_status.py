import requests
import pandas as pd

def extract_date(date_str):
    return date_str.split('T')[0]

def fetch_flight_status(flight_number, departure_date, headers):
    url = f"https://api.lufthansa.com/v1/operations/flightstatus/{flight_number}/{departure_date}"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        try:
            flight_status_data = response.json()
            print(f"Statut récupéré avec succès pour le vol {flight_number} du {departure_date}")
            return flight_status_data
        except ValueError:
            print(f"Erreur lors de l'analyse de la réponse JSON pour le vol {flight_number} du {departure_date}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération des données du statut pour le vol {flight_number} du {departure_date} : {e}")
        return None

def process_flight_status(flight_status_data, flight_number, departure_date):
    status_flight_info = []

    data_info = flight_status_data.get("FlightStatusResource", {}).get("Flights", {}).get("Flight", [])

    for status in data_info:
        status_flight_info.append({
            'FlightNumber': flight_number,
            'Departure Date': departure_date,
            'Departure AirportCode': status.get('Departure', {}).get('AirportCode', None),
            'Scheduled Departure Local Time': status.get('Departure', {}).get('ScheduledTimeLocal', {}).get('DateTime', None),
            'Actual Departure Local Time': status.get('Departure', {}).get('ActualTimeLocal', {}).get('DateTime', None),
            'Departure Terminal': status.get('Departure', {}).get('Terminal', {}).get('Name', None),
            'Departure Gate': status.get('Departure', {}).get('Terminal', {}).get('Gate', None),
            'Arrival AirportCode': status.get('Arrival', {}).get('AirportCode', None),
            'Scheduled Arrival Local Time': status.get('Arrival', {}).get('ScheduledTimeLocal', {}).get('DateTime', None),
            'Actual Arrival Local Time': status.get('Arrival', {}).get('ActualTimeLocal', {}).get('DateTime', None),
            'Arrival Terminal': status.get('Arrival', {}).get('Terminal', {}).get('Name', None),
            'Arrival Gate': status.get('Arrival', {}).get('Terminal', {}).get('Gate', None),
            'Time Status Code': status.get('Departure', {}).get('TimeStatus', {}).get('Code', None),
            'Time Status Definition': status.get('Departure', {}).get('TimeStatus', {}).get('Definition', None),
            'Marketing Airline ID': status.get('MarketingCarrier', {}).get('AirlineID', None),
            'Marketing Flight Number': status.get('MarketingCarrier', {}).get('FlightNumber', None),
            'Aircraft Code': status.get('Equipment', {}).get('AircraftCode', None),
            'Aircraft Registration': status.get('Equipment', {}).get('AircraftRegistration', None),
            'Flight Status Code': status.get('FlightStatus', {}).get('Code', None),
            'Flight Status Definition': status.get('FlightStatus', {}).get('Definition', None)
        })

    return status_flight_info

def process_flight_status_workflow(schedules_df, headers):
    status_flight_info = []

    for index, row in schedules_df.iterrows():
        flight_number = row['schedule_id']
        departure_date = extract_date(row['DateTime Dep'])

        flight_status_data = fetch_flight_status(flight_number, departure_date, headers)

        if flight_status_data:
            status_flight_info.extend(process_flight_status(flight_status_data, flight_number, departure_date))
            
    return pd.DataFrame(status_flight_info)