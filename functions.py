import json
import pandas as pd
import requests
import wbgapi as wb

def ember_fetch_data(ep, countries, key, base_url):
    """Fetch data from API for given countries and return as list."""
    all_data = []
    for country in countries:
        query_url = f'{base_url}/v1/{ep}?Code={country}&is_aggregate_series=false&start_date=2000&api_key={key}'
        response = requests.get(query_url)

        if response.status_code == 200:
            response_data = response.json()
            all_data.extend(response_data['data'])  # Store only 'data' part
        else:
            print(f"Warning: Failed to fetch data for {country}. Status Code: {response.status_code}")
    
    return all_data

def ember_filter_data(df):
    """Filter for years 2023 and 2024, and select relevant energy sources."""
    df_filtered = df[df['date'].isin(['2023', '2024'])]
    desired_series = ['Wind', 'Hydro', 'Solar', 'Bioenergy', 'Other renewables']
    return df_filtered[df_filtered['series'].isin(desired_series)]

def ember_aggregate_bioenergy_other_renewables(df):
    """Sum Bioenergy and Other renewables into 'Other renewables'."""
    bioenergy = df[df['series'] == 'Bioenergy']
    other_renewables = df[df['series'] == 'Other renewables']

    # Merge them on common keys
    combined = bioenergy.merge(
        other_renewables, 
        on=['entity', 'entity_code', 'date'], 
        suffixes=('_bio', '_other'), 
        how='outer'
    )

    # Sum values (handle missing values with fillna)
    combined['generation_twh'] = combined[['generation_twh_bio', 'generation_twh_other']].sum(axis=1, skipna=True)
    
    # Keep only relevant columns and rename
    combined = combined[['entity', 'entity_code', 'date', 'generation_twh']].copy()
    combined['series'] = 'Other renewables'

    # Remove original Bioenergy and Other renewables rows
    df = df[~df['series'].isin(['Bioenergy', 'Other renewables'])]

    # Append the new aggregated data
    return pd.concat([df, combined])

def ember_reshape_data(df):
    """Reshape data into a normal DataFrame with separate columns for each energy source."""
    # Define the expected energy sources
    energy_sources = ['Wind', 'Hydro', 'Solar', 'Other renewables']

    # Create a new DataFrame with one row per country-year
    df_final = df[['entity', 'entity_code', 'date']].drop_duplicates().reset_index(drop=True)

    # Add columns for each energy source and initialize with NaN
    for source in energy_sources:
        df_final[source] = None

    # Fill in the values from the original dataframe
    for _, row in df.iterrows():
        mask = (df_final['entity'] == row['entity']) & (df_final['date'] == row['date'])
        df_final.loc[mask, row['series']] = row['generation_twh']

    # Fill missing values with 0 (optional)
    df_final.fillna(0, inplace=True)

    # Rename columns to match df_energy_prod
    df_final = df_final.rename(columns={'entity': 'country', 'entity_code': 'iso_code', 'date': 'year', 'Wind': 'wind', 'Hydro': 'hydro', 'Solar': 'solar', 'Other renewables': 'other_inc_bio'})

    return df_final

def wb_get_data(indicator_dict, countries):
    dfs = []
    for column_name, api_indicator in indicator_dict.items():
        df = wb.data.DataFrame(api_indicator, countries, range(1965, 2023))
        df_stacked = df.stack().reset_index()
        df_stacked.columns = ['iso_code', 'year', column_name]
        df_stacked['year'] = df_stacked['year'].str.replace('YR', '')
        dfs.append(df_stacked)

    df_final = dfs[0]
    for df in dfs[1:]:
        df_final = df_final.merge(df, on=['iso_code', 'year'], how='outer')
        
    return df_final

def save_json(data, output_json_file):
    """Save data to a JSON file."""
    with open(output_json_file, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    print(f"Data successfully saved to {output_json_file}")

def load_json(input_json_file):
    """Load JSON data from file and return as a DataFrame."""
    with open(input_json_file, 'r') as json_file:
        all_data = json.load(json_file)
    return pd.DataFrame(all_data)