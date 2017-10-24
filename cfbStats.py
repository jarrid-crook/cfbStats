import csv
from bs4 import BeautifulSoup
import requests
import pandas as pd
import mysql.connector as mydb

#############################Edit As Needed#############################
year = '2017'
dataDir = '/home/bebop/projects/data/'
########################################################################

# Scrape Passing Stats

outfile = open(dataDir + year + 'pass.csv', 'w', newline='\n')
writer = csv.writer(outfile)
writer.writerow(["Rank", "Name", "Yr", "Pos", "G", "Att", "Comp", "Pct", "Yards", "Yards/Att", "Td", "Int", "Rating", "Att/G", "Yards/G", "TeamID", "Year"])
outfile.close()


with open(dataDir + 'teamIDs.txt', 'r') as f:
    ids = [line.strip() for line in f]    
    for line in ids:
        address = ('http://www.cfbstats.com/' + year + '/team/' + line + '/passing/index.html')
        response = requests.get(address)
        html = response.content

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', attrs={'class': 'leaders'})

        list_of_rows = []
        alltr = table.findAll('tr')
        for row in alltr[1:]:
            list_of_cells = []
            for cell in row.findAll('td'):
                text = cell.text.replace('&nbsp;', '').strip()
                list_of_cells.append(text)
            list_of_cells.append(line) 
            list_of_rows.append(list_of_cells)
        outfile = open(dataDir + year + 'pass.csv', 'a', newline='\n')
        writer = csv.writer(outfile)
        writer.writerows(list_of_rows)
        outfile.close()

csv_input = pd.read_csv(dataDir + year + 'pass.csv')
csv_input['Year'] = year
csv_input.to_csv(dataDir + year + 'pass.csv', index=False)


# Scrape Receiving Stats


#Load data to MySQL

db = mydb.connect(host="localhost",user="groot",password="g070291root")
cursor = db.cursor()

cursor.execute("""CREATE DATABASE IF NOT EXISTS CFB""")
cursor.execute("""USE CFB""")
cursor.execute("""CREATE TABLE IF NOT EXISTS passImport (Ranking INT, Name VARCHAR(30), Yr VARCHAR(10), Pos VARCHAR(20), Games INT, Att INT, Comp INT, Pct FLOAT, Yards INT, YardsAtt FLOAT, Td INT, Ints INT, Rating FLOAT, AttG FLOAT, YardsG FLOAT, TeamID VARCHAR(3), Year VARCHAR(4))""")

clearOld = """DELETE FROM passImport WHERE Year = "%s" """ % (year)
cursor.execute(clearOld)

loadPass = """LOAD DATA LOCAL INFILE "%s%s%s" INTO TABLE passImport FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS""" % (dataDir, year, 'pass.csv')
cursor.execute(loadPass)

cursor.execute("""DELETE FROM passImport WHERE Ranking = 0 OR Name = 'Team'""")


cursor.execute("""DROP TABLE IF EXISTS teamMap""")
cursor.execute("""CREATE TABLE teamMap (TeamID VARCHAR(3), Team VARCHAR(50), Conf VARCHAR(10))""")
loadMap = """LOAD DATA LOCAL INFILE "%s%s" IGNORE INTO TABLE teamMap FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (TeamID, Team, Conf)""" % (dataDir, 'teamMap.csv')
cursor.execute(loadMap)

db.commit()
cursor.close()
