# Code for ETL operations on Country-GDP data

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    r = requests.get(url).text
    soup = BeautifulSoup(r, 'html.parser')
    tables = soup.find_all('table')
    my_table = tables[0]
    table_rows = my_table.find_all('tr')
    without_header_table_rows = table_rows[1:]
    country = []
    m_cap = []
    dictt = {}
    for table_row in without_header_table_rows:
        table_cells = table_row.find_all('td')
        m_cap.append(float(table_cells[2].string.rstrip()))
    
    for table_row in without_header_table_rows:
        table_cells = table_row.find_all('td')
        for anchor_tags in table_cells:
            anchors = anchor_tags.find_all('a')
            if len(anchors) >= 1:
                country.append(anchors[1].string)

    dictt['Name'] = country
    dictt['MC_USD_Billion'] = m_cap
    df = pd.DataFrame(dictt)

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    with open(exchange_rate_csv, 'r') as f:
        csv_reader = csv.reader(f)
        next_csv_reader = next(csv_reader)
        exchange_rate = {}
        
        for row in csv_reader:
            key = row[0]
            value = row[1]
            exchange_rate[key] = float(value)
            
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Importing the required libraries

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import csv



# ''' Here, you define the required entities and call the relevant
# functions in the correct order to complete the project. Note that this
# portion is not inside any function.'''


url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attrs_upon_extr = ['Name', 'MC_USD_Billion']
csv_path = './Largest_banks_data.csv'
exchange_rate_csv = './exchange_rate.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
df_upon_extr = pd.DataFrame(columns=table_attrs_upon_extr)

log_progress('Preliminaries complete. Initiating ETL process')

extracted_data = extract(url, table_attrs_upon_extr)
# print(extracted_data)

log_progress('Data extraction complete. Initiating Transformation process')

transformed_data = transform(extracted_data, exchange_rate_csv)
print(transformed_data)

log_progress('Data transformation complete. Initiating Loading process')

print(transformed_data['MC_EUR_Billion'][4])

load_to_csv(transformed_data, csv_path)

log_progress('Data saved to CSV file')

conn = sqlite3.connect(db_name)

log_progress('SQL Connection initiated')

load_to_db(transformed_data, conn, table_name)

log_progress('Data loaded to Database as a table, Executing queries')

query1 = 'SELECT * FROM Largest_banks'
query2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query3 = 'SELECT Name from Largest_banks LIMIT 5'

print(run_query(query1, conn))
print(run_query(query2, conn))
print(run_query(query3, conn))

log_progress('Process Complete')

conn.close()

log_progress('Server Connection closed')