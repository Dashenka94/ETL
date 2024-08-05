print('start')
import pandas as pd
from functions import get_latest_date, make_correct_type, insert_data_to_mysql, read_data, get_data, get_plot_data



def main():

    #we need a feature for new tickers bwecause code is not working when we enter new ticker






    latest_date_table = get_latest_date()


    db_table = []
    for ticker in stock_ticker:
        for row in latest_date_table:
            print(row)
            if ticker == row[1]:
                print(f'{ticker} Started')
                stock_name, table = get_data(ticker, row[0])
                print(f'{ticker} Fetched')


                db_table.append(table)

            else:
                pass



    stock_data = pd.concat(db_table, axis=0)
    data_with_correct_type = make_correct_type(stock_data)
    stock_data_reindexed = (stock_data.reset_index(drop=['index']))
    insert_data_to_mysql(stock_data_reindexed)
    red_data = read_data(stock_data_reindexed)
    print('YOU made it!!!!!!!!!')




if __name__ == '__main__':
    main()
    get_plot_data(200)

