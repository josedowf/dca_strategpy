from utils import read_creds
import json
import psycopg2
import pandas as pd


class Table:
    def __init__(self, ticker):
        self.ticker = ticker

    def stock_template(self):
        return f'''CREATE TABLE IF NOT EXISTS {self.ticker}_stock_data (
                                id              BIGINT,
                                ticker          VARCHAR(10),
                                adjusted        BOOLEAN,
                                volume          FLOAT,
                                wap_volume      FLOAT,
                                open_price      FLOAT,
                                close_price     FLOAT,
                                high_price      FLOAT,
                                low_price       FLOAT,
                                timestamp       TIMESTAMP PRIMARY KEY,
                                transactions    BIGINT
                                )'''


with open('creds.json', 'r') as f:
    cred = json.load(f)


# connects and loads the provided data to its corresponding sql db
def load_stock_data_to_sql(ticker, ticker_info_list, last_id, creds: str = r'' + cred['main_path']
                                                                           + 'stock_market/creds.json'):
    # Ensure ticker is all caps to standardize names in database
    ticker = ticker.upper()

    # authenticate and assign credentials to SQL to execute query.
    creds = read_creds(creds)
    db_host, db_name, port_id = creds['db_host'], creds['db_name'], creds['port_id']
    db_user, db_pass = creds['db_user'], creds['db_pass']

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
                            host=db_host,
                            dbname=db_name,
                            user=db_user,
                            password=db_pass,
                            port=port_id)

        cur = conn.cursor()

        table = Table(ticker)
        create_table_if_not_exists = table.stock_template()

        cur.execute(create_table_if_not_exists)

        if last_id == 1:
            start_id_range = 1
            end_id_range = len(ticker_info_list) + 1

            ticker_info_list.insert(0, 'id', range(start_id_range, end_id_range))

            rows_to_insert = [tuple(ticker_info[i] for i in range(len(ticker_info_list.columns)))
                              for ticker_info in ticker_info_list.values]
        else:
            start_id_range = int(last_id) + 1
            end_id_range = int(last_id) + len(ticker_info_list) + 1

            ticker_info_list.insert(0, 'id', range(start_id_range, end_id_range))

            get_last_timestamp = f''' SELECT Timestamp FROM {ticker}_stock_data 
                              ORDER BY Timestamp DESC  
                              LIMIT 1
                                '''

            cur.execute(get_last_timestamp)

            last_timestamp = cur.fetchone()[0]
            print(f'Last recorded event: {last_timestamp}')

            ticker_info_list['Timestamp'] = pd.to_datetime(ticker_info_list['Timestamp'])

            # Filter the rows_to_insert list to only include rows with a timestamp greater than or equal to the cutoff
            rows_to_insert = [tuple(ticker_info[i] for i in range(len(ticker_info_list.columns)))
                              for i, ticker_info in enumerate(ticker_info_list.values) if
                              ticker_info_list.loc[i, 'Timestamp'] > last_timestamp]

        if len(rows_to_insert) > 0:
            print(f'{len(rows_to_insert)} new rows of data to be loaded to SQL.')
            print()
            # Insert query that ensures that all information loaded to SQL is not repeated
            insert_script = f'''INSERT INTO {ticker}_stock_data (id, ticker, adjusted, volume, wap_volume, open_price, 
                                       close_price, high_price, low_price, timestamp, transactions) 
                                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                       ON CONFLICT DO NOTHING'''

            cur.executemany(insert_script, rows_to_insert)

            conn.commit()
            print('Successful SQL posting')
            print(f'Loaded {ticker}_stock_data table with information')
            print()
        else:
            print('Information up to date. No data to load.')
            print()
    except Exception as error:
        print('Failed to connect to SQL database')
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


# retrieve the last id in sql for a particular db. Used for correctly indexing dbs.
def set_id_column(ticker, creds: str = r'' + cred['main_path'] + 'stock_market/creds.json'):
    # authenticate and assign credentials to SQL to execute query.
    creds = read_creds(creds)
    db_host, db_name, port_id = creds['db_host'], creds['db_name'], creds['port_id']
    db_user, db_pass = creds['db_user'], creds['db_pass']

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
                            host=db_host,
                            dbname=db_name,
                            user=db_user,
                            password=db_pass,
                            port=port_id)

        cur = conn.cursor()

        table = Table(ticker)
        create_table_if_not_exists = table.stock_template()

        cur.execute(create_table_if_not_exists)

        get_last_id = f''' SELECT COALESCE(MAX(id), 1) FROM {ticker}_stock_data;
                            '''

        cur.execute(get_last_id)
        last_id = cur.fetchone()
        last_id = last_id[0]

        return last_id

    except Exception as error:
        print("Failed to retrieve last id from dataset")
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def extract_stock_data_to_date(ticker, start_date, creds: str = r'' + cred['main_path'] + 'stock_market/creds.json'):
    # authenticate and assign credentials to SQL to execute query.
    creds = read_creds(creds)
    db_host, db_name, port_id = creds['db_host'], creds['db_name'], creds['port_id']
    db_user, db_pass = creds['db_user'], creds['db_pass']

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
                            host=db_host,
                            dbname=db_name,
                            user=db_user,
                            password=db_pass,
                            port=port_id)

        cur = conn.cursor()

        get_last_stock_data = f''' SELECT * FROM {ticker}_stock_data 
                            WHERE timestamp >= '{start_date}'
                            ORDER BY timestamp DESC  
                            '''

        cur.execute(get_last_stock_data)
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]

        stock_data = pd.DataFrame(rows, columns=headers)
        print('Extracted stock data from SQL database.')
        print()
        return stock_data
    except Exception as error:
        print("Failed to retrieve last id from dataset")
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
