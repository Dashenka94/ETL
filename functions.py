import requests
import pandas as pd
import json
import datetime as dt
import mysql.connector
import matplotlib.pyplot as plt
from datetime import timedelta
import mplfinance as mpf
import plotly.graph_objects as go
import plotly.subplots as ms


def get_latest_date():

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="emplifi_install2",
        database="GLASS"
    )
    cursor = conn.cursor(buffered=True)

    cursor.execute("select max(date), stock from GLASS.daily_data group by stock")

    rows = cursor.fetchall()

    latest_date_table = []
    for row in rows:
        print(row)
        latest_date_table.append(row)

    cursor.close()
    conn.close()
    return latest_date_table



def insert_data_to_mysql(stock_data_reindexed):
    cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="emplifi_install2",
        database="GLASS"

    )
    mycursor = cnx.cursor(buffered=True)

    query = "INSERT IGNORE INTO GLASS.daily_data VALUES (%s,%s,%s,%s,%s,%s,%s)"

    mycursor.executemany(query, stock_data_reindexed.values.tolist())

    cnx.commit()
    return True


def read_data(stock_data_reindexed):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="emplifi_install2",
        database="GLASS"

    )
    cursor = conn.cursor(buffered=True)

    cursor.execute("SELECT * FROM GLASS.daily_data")

    rows = cursor.fetchall()

    saved_rows = []
    for row in rows:
        print(row)
        saved_rows.append(row)

    df_saved_rows = pd.DataFrame(saved_rows)
    df_saved_rows = df_saved_rows.rename(columns={0: 'date',1:'close', 2: 'volume', 3: 'open', 4: 'high', 5: 'low', 6: 'stock'})

    cursor.close()
    conn.close()
    return df_saved_rows


    print('You are doing great!')




def make_correct_type(stock_data):
    print('You can do it!!')
    stock_data[['close', 'volume', 'open', 'high', 'low']] = stock_data[
        ['close', 'volume', 'open', 'high', 'low']].replace({',': '', '\$': '', ' ': '', 'N/A': '0'}, regex=True)

    stock_data['date'] =stock_data['date'].replace({',': '', '\$': '', ' ': '', 'N/A': ''}, regex=True)

    stock_data['date'] = pd.to_datetime(stock_data['date'])

    stock_data[['close', 'volume', 'open', 'high', 'low']] = stock_data[
        ['close', 'volume', 'open', 'high', 'low']].astype(float)
    return stock_data




def get_data(ticker, latest_date):

    value_d = latest_date - timedelta(days=7)

    value_datetime = value_d.strftime('%Y-%m-%d')



    today = dt.date.today()

    url = f"https://api.nasdaq.com/api/quote/{ticker}/historical"

    # payload and headers are like words for requests to server for starting communication
    payload = {'assetclass':'stocks','fromdate':f'{value_datetime}','limit':'5000','todate':f'{today}'}

    headers = {'Accept': 'application/json',
               'Accept-Encoding': 'gzip, deflate, br',
               'Connection': 'keep-alive',
               'Host': "api.nasdaq.com",
               'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
            }
    # I'm sending a request to the server
    my_request_to_server = requests.get(url=url,headers=headers,params=payload)
    # status_code should be 200
    if my_request_to_server.status_code != 200:
        print("No respond from server!")

    #Convert request data to pandas using the steps we followed to convert types. Take symbol and tradesTable from server response
    else:
        reply_from_server = json.loads(my_request_to_server.content) # I'm converting json file from server into python object, in other words - it is a reply from server
        # now i need to take particular data for table from the reply_from_server, to do so I need access a values within a nested dictionary structure
        stock_name = reply_from_server['data']['symbol']
        main_data = reply_from_server['data']['tradesTable']['rows']


        # I need to create a table from main_table that we access the last step
        ready_table = pd.DataFrame(main_data)

        # I need to add a column 'stock name'
        ready_table['stock'] = stock_name

        # now return whole table and stock name back where we started
        return stock_name, ready_table



def get_plot_data(days):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="emplifi_install2",
        database="GLASS"

    )
    cursor = conn.cursor(buffered=True)

    cursor.execute("select * from GLASS.daily_data order by date desc limit 120")

    rows = cursor.fetchall()

    selected = []
    for row in rows:
        print(row)
        selected.append(row)

    cursor.close()
    conn.close()


    selected_df = pd.DataFrame(selected)
    selected_df = selected_df.rename(columns={0: 'date', 1: 'close', 2: 'volume', 3: 'open', 4: 'high', 5: 'low', 6: 'stock'})


    stock_ticker = ["QBTS", "CRSP"]


    for ticker in stock_ticker:
        df1 = selected_df[selected_df['stock'] == ticker]
        df1['date'] = pd.to_datetime(df1['date'])

        fig = ms.make_subplots(rows=2,
                               cols=1,
                               shared_xaxes=True,
                               vertical_spacing=0.02)
        fig.add_trace(go.Candlestick(
            x=df1['date'],
            open=df1['open'],
            high=df1['high'],
            low=df1['low'],
            close=df1['close']),
        row=1,
        col=1
        )

        # Add Volume Chart to Row 2 of subplot
        fig.add_trace(go.Bar(x=df1.index,
                             y=df1['volume']),
        row=2,
        col=1
                      )

        # Update Price Figure layout
        fig.update_layout(title='Interactive',
        yaxis1_title = 'Stock',
        yaxis2_title = 'Volume',
        xaxis1_rangeslider_visible = False,
        xaxis2_rangeslider_visible = False)
        fig.show()


    plt.show()
    plt.close()



