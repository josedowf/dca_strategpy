import json
import requests
import pandas as pd


# date range format must be in YYYY-MM-DD
def stock_data_request(ticker, start_date, end_date):
    with open('creds.json', 'r') as f:
        creds = json.load(f)

    # Set the API key
    api_key = creds['api_key']
    polygon_url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?adjusted=' \
                  f'true&sort=asc&limit=5000&apiKey='

    endpoint = polygon_url + api_key
    response = requests.get(endpoint)

    # Check if the response is successful
    if response.status_code != 200:
        # Print the error message if the response is unsuccessful
        print(f'Request failed with status code {response.status_code}')
        print(response.text)
    else:
        # Convert the response to JSON format
        data = json.loads(response.text)

        df = pd.DataFrame(data)

        # Drop unnecessary columns
        df = df.drop(['queryCount', 'resultsCount', 'status', 'request_id', 'count'], axis=1)

        # Normalize the "results" column
        df_results = pd.json_normalize(df['results'])

        # Join the normalized data with the original DataFrame
        df = pd.concat([df, df_results], axis=1)

        # Drop the original "results" column
        df = df.drop('results', axis=1)

        # convert the unix_timestamp column to datetime objects
        df['t'] = pd.to_datetime(df['t'], unit='ms')

        df = df.rename(columns={
            'v': 'Volume',
            'vw': 'Wap_volume',
            'o': 'Open_price',
            'c': 'Close_price',
            'h': 'High_price',
            'l': 'Low_price',
            't': 'Timestamp',
            'n': 'Transactions'
        })

        return df
        # df.to_csv('output.csv', index=False)

