from sql_conn import load_stock_data_to_sql, set_id_column
from stock_downloader import stock_data_request

ticker = 'AAPL'
start_range = '2020-05-28'
end_range = '2023-05-28'

stock_data = stock_data_request(ticker, start_range, end_range)

load_stock_data_to_sql(ticker, stock_data, set_id_column(ticker))

stock_data.to_csv(f'{ticker}_stock_data.csv', index=False)


