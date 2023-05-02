import pandas as pd
import os
from sql_conn import extract_stock_data_to_date, load_stock_data_to_sql, set_id_column
from utils import stock_data_request, dca_strategy, today_formatted
import time

start_range = '2013-01-01'
dca_periodicity = 'W'  # Select your strategy! Weekly: W, Monthly: M, Quarterly: Q, Bi-Annually: 2Q or Annually: A
dc_amount = 75  # Select the amount to recurrently invest
today = today_formatted()

fortune500_tickers = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'TSLA', 'NVDA', 'JPM', 'JNJ', 'V', 'KO',
                      'PG', 'WMT', 'HD', 'MA', 'DIS', 'PYPL', 'BAC', 'NFLX', 'VZ', 'CMCSA', 'SPY']


# Load all stock data from polygon to a SQL database in each corresponding table
for ticker in fortune500_tickers:
    stock_data = stock_data_request(ticker, start_range, today)
    load_stock_data_to_sql(ticker, stock_data, set_id_column(ticker))
    time.sleep(20)

# Extract stock data from each table, run them through the dca strategy and store
# the results in csv files
directory = 'dca_strategy_companies_dir'

for ticker in fortune500_tickers:
    stock_data = extract_stock_data_to_date(ticker, start_range)
    dca_results = dca_strategy(stock_data, dca_periodicity, dc_amount)
    dca_results[0].to_csv(f'{directory}/{ticker}_dca_strategy.csv', index=False)

# Create an array with all the file names in order to concatenate all the results into one single csv
csv_files = [file for file in os.listdir(directory) if file.endswith('.csv')]
csv_array = {}

for file in csv_files:
    df = pd.read_csv(os.path.join(directory, file))
    variable_name = file.replace('.csv', '')
    csv_array[variable_name] = df

combined_csv = pd.concat(csv_array, ignore_index=True)
combined_csv.to_csv(f'combined_dca_strategy_info_{today}.csv', index=False)



