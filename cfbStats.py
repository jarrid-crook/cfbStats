import csv
from bs4 import BeautifulSoup
import requests
import pandas as pd
import mysql.connector as mydb
import os, errno

#############################Edit As Needed#############################
year = '2016'
dataDir = '/mnt/disks/disk1/bebop/projects/cfbStats/data/'
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


def yearApp(y):
    csv_input = pd.read_csv(dataDir + year + y + '.csv',sep='|')
    csv_input[year] = year
    csv_input.to_csv(dataDir + year + y + '.csv', index=False, sep='|')


def executeScriptsFromFile(filename):
    opFile = open(filename, 'r')
    sqlFile = opFile.read()
    opFile.close()
    sqlCommands = sqlFile.split(';')
    for command in sqlCommands:
        try:
            if command.strip() != '':
                cursor.execute(command)
        except IOError:
            print("Command skipped: ")

###########################################################################################################################

# Remove Files If They Exist

# try:
os.remove(dataDir + year + 'passing.csv')
# except OSError:
#     pass




# Scrape Passing Stats

try:
    scraper('passing','passing')
except:
    pass

yearApp('passing')


# Scrape Receiving Stats

try:
    scraper('receiving','receiving')
except:
    pass

yearApp('receiving')


# Scrape Rushing Stats

try:
    scraper('rushing','rushing')
except:
    pass

yearApp('rushing')


# Scrape Punt Return Stats

try:
    scraper('puntReturn','puntreturn')
except:
    pass


# Scrape Kickoff Return Stats

try:
    scraper('kickReturn','kickreturn')
except:
    pass


# Load data to MySQL

db = mydb.connect(host="localhost",user="groot",password="i070291BM!")
cursor = db.cursor()

def executeScriptsFromFile(filename):
    opFile = open(filename, 'r')
    sqlFile = opFile.read()
    opFile.close()
    sqlCommands = sqlFile.split(';')

    for command in sqlCommands:
        try:
            if command.strip() != '':
                cursor.execute(command)
        except IOError:
            print("Command skipped: ")

executeScriptsFromFile('/mnt/disks/disk1/bebop/projects/cfbStats/dbCfbStats.sql')

db.commit()
cursor.close()
