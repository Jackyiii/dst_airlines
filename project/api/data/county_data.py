import requests
import pandas as pd

def fetch_country_data(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}. Error: {e}")
        return None

def process_country_data(country_data):
    all_country_info = []
    data_info = country_data['CountryResource']['Countries']['Country']
    for country in data_info:
        country_code = country['CountryCode']
        names = country['Names']['Name']
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
            language_code = names['@LanguageCode']
            country_name = names['$']
            all_country_info.append({
                'CountryCode': country_code,
                'LanguageCode': language_code,
                'CountryName': country_name
            })
    return all_country_info

def create_country_dataframe(all_country_info):
    return pd.DataFrame(all_country_info)

def filter_countries_with_language(df, language_code):
    countries_with_lang = df.groupby('CountryCode').apply(
        lambda group: language_code in group['LanguageCode'].values
    )
    return countries_with_lang[countries_with_lang].index.tolist()


def process_country_data_workflow(headers, urls):
    """
    Workflow to fetch and process country data from the API.

    Args:
    headers (dict): The headers used for API requests.
    urls (list): The list of URLs for fetching country data.

    Returns:
    pd.DataFrame: DataFrame containing country data filtered by 'EN' language.
    """
    all_country_info = []

    # Fetch and process country data from each URL
    for url in urls:
        country_data = fetch_country_data(url, headers)
        if country_data:
            country_info = process_country_data(country_data)
            all_country_info.extend(country_info)

    # Create DataFrame from all country info
    country_df = create_country_dataframe(all_country_info)

    # Print the DataFrame
    print(country_df)

    # Filter countries with 'EN' as a language
    countries_with_en = filter_countries_with_language(country_df, 'EN')
    print("Countries with 'EN' as a language:", countries_with_en)

    # Filter DataFrame to only include countries with 'EN'
    df_with_en_language = country_df[country_df['CountryCode'].isin(countries_with_en)]
    print("\nDataFrame with countries having 'EN' as a language:\n", df_with_en_language)

    # Filter rows with LanguageCode == 'EN'
    country_df_final_lang_en = df_with_en_language.loc[df_with_en_language['LanguageCode'] == 'EN']
    print(country_df_final_lang_en.head())

    return country_df_final_lang_en