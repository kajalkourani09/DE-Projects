
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

def log_progress(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open("code_log.txt", "a") as f:
        f.write(f"{timestamp} : {message}\n")

log_progress("Preliminaries complete. Initiating ETL process")

def extract():
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all('table')[1]
    rows = table.find_all('tr')
    data = []
    for row in rows[1:11]:  # top 10 banks
        cols = row.find_all('td')
        if cols:
            name = cols[1].text.strip()
            mc = float(cols[2].text.strip().replace('\n', '').replace(',', ''))
            data.append([name, mc])
    df = pd.DataFrame(data, columns=['Name', 'MC_USD_Billion'])
    return df


df = extract()
log_progress("Data extraction complete. Initiating Transformation process")

def transform(df):
    exchange_df = pd.read_csv("exchange_rate.csv")
    exchange_rate = dict(zip(exchange_df['Currency'], exchange_df['Rate']))
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    return df

df = transform(df)
log_progress("Data transformation complete. Initiating Loading process")

def load_to_csv(df):
    df.to_csv("Largest_banks_data.csv", index=False)
    log_progress("Data saved to CSV file")

load_to_csv(df)

conn = sqlite3.connect("Banks.db")
log_progress("SQL Connection initiated")

def load_to_db(df, conn, table_name):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")

load_to_db(df, conn, "Largest_banks")

def run_queries(conn):
    queries = [
        "SELECT * FROM Largest_banks",
        "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
        "SELECT Name FROM Largest_banks LIMIT 5"
    ]
    for query in queries:
        print(f"\nQuery: {query}")
        result = pd.read_sql(query, conn)
        print(result)
    log_progress("Process Complete")
    conn.close()
    log_progress("Server Connection closed")

run_queries(conn)
