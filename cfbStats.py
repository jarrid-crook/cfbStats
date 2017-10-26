import csv
from bs4 import BeautifulSoup
import requests
import pandas as pd
import mysql.connector as mydb

#############################Edit As Needed#############################
year = '2017'
dataDir = '/home/bebop/projects/data/'
######################################################################



def scraper(x,y):
    with open(dataDir + 'teamIDs.txt', 'r') as f:
     ids = [line.strip() for line in f]
     for line in ids:
         address = ('http://www.cfbstats.com/' + year + '/team/' + line + '/' + x + '/' + '/index.html')
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
         outfile = open(dataDir + year + y + '.csv', 'a', newline='\n')
         writer = csv.writer(outfile, delimiter='|')
         writer.writerows(list_of_rows)
         outfile.close()

    csv_input = pd.read_csv(dataDir + year + y + '.csv',sep='|')
    csv_input['Year'] = year
    csv_input.to_csv(dataDir + year + y + '.csv', index=False, sep='|')


# Scrape Passing Stats

outfile = open(dataDir + year + 'passing.csv', 'w', newline='\n')
writer = csv.writer(outfile, delimiter='|')
writer.writerow(["Rank", "Name", "Yr", "Pos", "G", "Att", "Comp", "Pct", "Yards", "Yards/Att", "Td", "Int", "Rating", "Att/G", "Yards/G", "TeamID", "Year"])
outfile.close()

scraper('passing','passing')


# Scrape Receiving Stats

outfile = open(dataDir + year + 'receiving.csv', 'w', newline='\n')
writer = csv.writer(outfile,delimiter='|')
writer.writerow(["Rank", "Name", "Yr", "Pos", "G", "Rec", "Yards", "Avg", "TD", "Rec/G", "Yards/G", "TeamID", "Year"])
outfile.close()

scraper('receiving','receiving')


# Scrape Rushing Stats

outfile = open(dataDir + year + 'rushing.csv', 'w', newline='\n')
writer = csv.writer(outfile,delimiter='|')
writer.writerow(["Rank", "Name", "Yr", "Pos", "G", "Att", "Yards", "Avg", "TD", "Att/G", "Yards/G", "TeamID", "Year"])
outfile.close()

scraper('rushing','rushing')


# Scrape Punt Return Stats

#outfile = open(dataDir + year + 'puntReturn.csv', 'w', newline='\n')
#writer = csv.writer(outfile,delimiter='|')
#writer.writerow(["Rank", "Name", "Yr", "Pos", "G", "Ret", "Yards", "Avg", "TD", "Ret/G", "Yards/G", "TeamID", "Year"])
#outfile.close()
#
#scraper('puntReturn','puntreturn')
#
#
## Scrape Kickoff Return Stats
#
#outfile = open(dataDir + year + 'kickReturn.csv', 'w', newline='\n')
#writer = csv.writer(outfile,delimiter='|')
#writer.writerow(["Rank", "Name", "Yr", "Pos", "G", "Ret", "Yards", "Avg", "TD", "Ret/G", "Yards/G", "TeamID", "Year"])
#outfile.close()
#
#scraper('kickReturn','kickreturn')



# Load data to MySQL

db = mydb.connect(host="localhost",user="groot",password="g070291root")
cursor = db.cursor()

cursor.execute("""CREATE DATABASE IF NOT EXISTS CFB""")
cursor.execute("""USE CFB""")

# Create Tables

cursor.execute("""CREATE TABLE IF NOT EXISTS PASSING (Ranking INT, Name VARCHAR(30), Yr VARCHAR(10), Pos VARCHAR(20), Games INT, Att INT, Comp INT, Pct FLOAT, Yards INT, YardsAtt FLOAT, Td INT, Ints INT, Rating FLOAT, AttG FLOAT, YardsG FLOAT, TeamID VARCHAR(3), Year VARCHAR(4))""")

cursor.execute("""CREATE TABLE IF NOT EXISTS RECEIVING (Ranking INT, Name VARCHAR(30), Yr VARCHAR(10), Pos VARCHAR(20), Games INT, Receiving INT, Yards INT, Avg FLOAT, TD INT, RecG FLOAT, YardsG FLOAT, TeamID VARCHAR(3), Year VARCHAR(4))""")

# Clear Out Data To Be Updated

clearOld = """DELETE PASSING, RECEIVING FROM PASSING INNER JOIN RECEIVING WHERE PASSING.Year = RECEIVING.Year AND PASSING.Year = "%s" """ % (year)
cursor.execute(clearOld)

# Import Data

loadPass = """LOAD DATA LOCAL INFILE "%s%s%s" INTO TABLE PASSING FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' IGNORE 1 ROWS""" % (dataDir, year, 'passing.csv')
cursor.execute(loadPass)

loadReceive = """LOAD DATA LOCAL INFILE "%s%s%s" INTO TABLE RECEIVING FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' IGNORE 1 ROWS""" % (dataDir, year, 'receiving.csv')
cursor.execute(loadReceive)

# Remove Junk Lines

cursor.execute("""DELETE FROM PASSING WHERE Ranking = 0 OR Name = 'Team'""")
cursor.execute("""DELETE FROM RECEIVING WHERE Ranking = 0 OR Name = 'Team'""")

cursor.execute("""DROP TABLE IF EXISTS TEAMMAP""")
cursor.execute("""CREATE TABLE TEAMMAP (TeamID VARCHAR(3), Team VARCHAR(50), Conf VARCHAR(10))""")
loadMap = """LOAD DATA LOCAL INFILE "%s%s" IGNORE INTO TABLE TEAMMAP FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (TeamID, Team, Conf)""" % (dataDir, 'teamMap.csv')
cursor.execute(loadMap)

db.commit()
cursor.close()
