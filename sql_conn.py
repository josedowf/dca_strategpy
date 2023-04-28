from utils import read_creds
import json
import psycopg2
import pandas as pd

with open('creds.json', 'r') as f:
    cred = json.load(f)


# connects and loads the tweet to the sql db
def load_stock_data_to_sql(ticker, ticker_info_list, last_id, creds: str = r'' + cred['main_path'] + 'stock_market/creds.json'):
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

        create_table = f''' CREATE TABLE IF NOT EXISTS {ticker}_stock_data (
                            id              BIGINT PRIMARY KEY,
                            ticker          VARCHAR(10),
                            adjusted        BOOLEAN,
                            volume          FLOAT,
                            wap_volume      FLOAT,
                            open_price      FLOAT,
                            close_price     FLOAT,
                            high_price      FLOAT,
                            low_price       FLOAT,
                            timestamp       TIMESTAMP,
                            transactions    BIGINT
                            )'''

        cur.execute(create_table)

        if last_id == 1:
            start_id_range = 1
            end_id_range = len(ticker_info_list) + 1
        else:
            start_id_range = int(last_id[0][0]) + 1
            end_id_range = int(last_id[0][0]) + len(ticker_info_list) + 1

        # ticker_info_list.insert(0, 'id', range(last_id + 1, last_id + len(ticker_info_list) + 1))
        ticker_info_list.insert(0, 'id', range(start_id_range, end_id_range))

        insert_script = f'INSERT INTO {ticker}_stock_data (id, ticker, adjusted, volume, wap_volume, open_price, ' \
                        'close_price, high_price, low_price, timestamp, transactions) ' \
                        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        rows_to_insert = [tuple(ticker_info[i] for i in range(len(ticker_info_list.columns)))
                          for ticker_info in ticker_info_list.values]

        print(pd.DataFrame(rows_to_insert))
        cur.executemany(insert_script, rows_to_insert)

        conn.commit()
        print('Successful SQL posting')
    except Exception as error:
        print('Failed to connect to SQL database')
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


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

        get_last_id = f''' SELECT id FROM {ticker}_stock_data 
                          ORDER BY id DESC  
                          LIMIT 1
                            '''

        cur.execute(get_last_id)
        last_id = cur.fetchall()

        return last_id
    except Exception as error:
        print("Failed to retrieve last id from dataset")
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
    return 1
