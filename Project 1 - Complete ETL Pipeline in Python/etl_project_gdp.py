import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    r = requests.get(url).text
    soup = BeautifulSoup(r, 'html.parser')
    tables = soup.find_all('table')
    my_table = tables[2]
    trs = my_table.find_all('tr')
    trs_countries = trs[3:]
    countries = []
    gdps = []
    dictt = {}
    for row in trs_countries:
        tds = row.find_all('td')
        if '—' not in tds[2]:
            countries.append(tds[0].find('a').string)
            gdps.append(tds[2].string)
    dictt['Country'] = countries
    dictt['GDP_USD_billion'] = gdps
    df = pd.DataFrame(dictt)
    
    return df



def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    cur_col = list(df['GDP_USD_billion'])
    cur_coll = []
    for i in cur_col:
        tri = float(i.replace(',', ''))
        tri_bil = np.round(tri/1000 , 2)
        cur_coll.append(tri_bil)
    df['GDP_USD_billion'] = cur_coll
    return df



def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)



def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)



def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution 
    to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

db_name = 'database file World_Economies.db'
csv_path = 'Countries_by_GDP.csv'
table_name = 'Countries_by_GDP'
table_attribs = ['Country', 'GDP_USD_billion']
df = pd.DataFrame(columns=table_attribs)

log_progress('Preliminaries complete. Initiating ETL process.')

extracted_file = extract(url, table_attribs)

log_progress("Data extraction complete. Initiating Transformation process.")

transformed_file = transform(extracted_file)

log_progress("Data transformation complete. Initiating loading process.")

load_to_csv(transformed_file, csv_path)

log_progress('Data saved to CSV file.')

conn = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(transformed_file,conn,table_name)

log_progress('Data loaded to Database as table. Running the query.')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billion >= 100"

run_query(query_statement,conn)

conn.close()