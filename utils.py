import json
import datetime as dt
import requests
import pandas as pd


# read credentials from creds.json file
def read_creds(filename):
    # Read JSON file to load credentials.
    # Store API credentials in a safe place.
    # If you use Git, make sure to add the file to .gitignore
    with open(filename) as f:
        creds = json.load(f)
    return creds


def today_formatted():
    date = dt.datetime.today()
    formatted_date = date.strftime('%Y-%m-%d')
    return formatted_date


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
        df['t'] = pd.to_datetime(df['t'], unit='ms').dt.strftime('%Y-%m-%d')

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


def dca_strategy(stock_data, periodicity, dc_amount):

    # Validate periodicity
    allowed_periods = ['W', 'M', 'Q', '2Q', 'A']
    if periodicity not in allowed_periods:
        raise ValueError(f"Periodicity must be one of {', '.join(allowed_periods)}")

    # Resample the data to the desired periodicity
    stock_data.set_index('timestamp', inplace=True)
    stock_data = stock_data.resample(periodicity).last().reset_index()

    # Column that shows the amount of stock purchased per purchase event
    stock_data['stock_purchase_amount'] = dc_amount / stock_data['close_price']

    # Get the current close_price and total stock
    current_close_price = stock_data.loc[stock_data['timestamp'] == stock_data['timestamp'].max(), 'close_price'].values[0]
    total_stock = round(stock_data['stock_purchase_amount'].sum(), 2)

    stock_data['cumulative total'] = stock_data['stock_purchase_amount'].cumsum() * stock_data['close_price']

    total_investment = stock_data['timestamp'].count() * dc_amount
    final_investment_value = round(total_stock * current_close_price, 2)
    percentage_change = '{:.2%}'.format(final_investment_value / total_investment - 1)

    return stock_data, total_investment, final_investment_value, percentage_change, total_stock


