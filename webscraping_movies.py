import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

# Initialization
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_path = '/home/project/top_50_films.csv'
df = pd.DataFrame(columns=["Average Rank", "Film", "Year"])
count = 0

# Load and parse the HTML page
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

# Find the table body and its rows
tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

# Loop to extract top 50 entries
for row in rows:
    if count < 50:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
                "Average Rank": col[0].contents[0],
                "Film": col[1].contents[0],
                "Year": col[2].contents[0]
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
            count += 1
    else:
        break

# Print extracted data
print(df)

# Save to CSV
df.to_csv(csv_path, index=False)

# Save to SQLite DB
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()
