import requests
import pandas as pd

def fetch_schedules_data(url, headers):
    schedules_info = []
    failed_urls = []

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            try:
                schedules_data = response.json()
            except ValueError:
                print(f"Erreur lors de l'analyse de la réponse JSON pour l'URL: {url}")
                failed_urls.append(url)
                return schedules_info, failed_urls

        else:
            print(f"Erreur lors de la récupération des données. Code de statut: {response.status_code} pour l'URL: {url}")
            failed_urls.append(url)
            return schedules_info, failed_urls

        if isinstance(schedules_data, dict) and "ScheduleResource" in schedules_data:
            schedule_resource = schedules_data["ScheduleResource"]

            if isinstance(schedule_resource, dict) and "Schedule" in schedule_resource:
                data_info = schedule_resource["Schedule"]

                if isinstance(data_info, list):
                    for schedule in data_info:
                        schedules_info.append(process_schedule(schedule))
                else:
                    failed_urls.append(url)
            else:
                failed_urls.append(url)
        else:
            failed_urls.append(url)

    except Exception as e:
        print(f"Erreur lors de la requête : {str(e)} pour l'URL: {url}")
        failed_urls.append(url)

    return schedules_info, failed_urls

def process_schedule(schedule):
    return {
        'Duration': schedule.get('TotalJourney', {}).get('Duration', None),
        'AirportCode Dep': schedule.get('Flight', {}).get('Departure', {}).get('AirportCode', None),
        'DateTime Dep': schedule.get('Flight', {}).get('Departure', {}).get('ScheduledTimeLocal', {}).get('DateTime', None),
        'Terminal Dep': schedule.get('Flight', {}).get('Departure', {}).get('Terminal', {}).get('Name', None),
        'AirportCode Arr': schedule.get('Flight', {}).get('Arrival', {}).get('AirportCode', None),
        'DateTime Arr': schedule.get('Flight', {}).get('Arrival', {}).get('ScheduledTimeLocal', {}).get('DateTime', None),
        'Terminal Arr': schedule.get('Flight', {}).get('Arrival', {}).get('Terminal', {}).get('Name', None),
        'AirlineID': schedule.get('Flight', {}).get('MarketingCarrier', {}).get('AirlineID', None),
        'FlightNumber': schedule.get('Flight', {}).get('MarketingCarrier', {}).get('FlightNumber', None),
        'AircraftCode': schedule.get('Flight', {}).get('Equipment', {}).get('AircraftCode', None),
        'StopQuantity': schedule.get('Flight', {}).get('Details', {}).get('Stops', {}).get('StopQuantity', None),
        'DaysOfOperation': schedule.get('Flight', {}).get('Details', {}).get('DaysOfOperation', None),
        'Effective': schedule.get('Flight', {}).get('Details', {}).get('DatePeriod', {}).get('Effective', None),
        'Expiration': schedule.get('Flight', {}).get('Details', {}).get('DatePeriod', {}).get('Expiration', None)
    }

def create_schedules_dataframe(schedules_info):
    return pd.DataFrame(schedules_info)

def process_schedules_workflow(headers, urls):
    all_schedules_info = []
    failed_urls = []

    for url in urls:
        schedules_info, failed = fetch_schedules_data(url, headers)
        all_schedules_info.extend(schedules_info)
        failed_urls.extend(failed)

    if all_schedules_info:
        schedules_df = create_schedules_dataframe(all_schedules_info)
        print(f"Total flights retrieved: {schedules_df.shape[0]}")
    else:
        
        schedules_df = pd.DataFrame()
        
    return schedules_df, failed_urls