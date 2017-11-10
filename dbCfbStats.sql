-- #######################Edit As Needed###################
SET @varYear = 2016;
-- ########################################################


CREATE DATABASE IF NOT EXISTS CFB;

USE CFB;

CREATE TABLE IF NOT EXISTS PASSING (
Ranking INT, 
Name VARCHAR(30), 
Yr VARCHAR(10), 
Pos VARCHAR(20), 
Games INT, 
Att INT, 
Comp INT, 
Pct FLOAT, 
Yards INT, 
YardsAtt FLOAT, 
Td INT, 
Ints INT, 
Rating FLOAT, 
AttG FLOAT, 
YardsG FLOAT, 
TeamID VARCHAR(3), 
Year VARCHAR(4)
);


CREATE TABLE IF NOT EXISTS RECEIVING (
Ranking INT, 
Name VARCHAR(30), 
Yr VARCHAR(10), 
Pos VARCHAR(20), 
Games INT, 
Receiving INT, 
Yards INT,
Avg FLOAT, 
TD INT, 
RecG FLOAT, 
YardsG FLOAT, 
TeamID VARCHAR(3), 
Year VARCHAR(4)
);


DELETE FROM PASSING WHERE Year = @varYear;
DELETE FROM RECEIVING WHERE Year = @varYear;
DELETE FROM PASSING WHERE Year = @varYear;
DELETE FROM PASSING WHERE Year = @varYear;


LOAD DATA INFILE
'/mnt/disks/disk1/bebop/projects/cfbStats/data/2016passing.csv'
INTO TABLE PASSING 
FIELDS TERMINATED BY '|' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;


DELETE FROM PASSING WHERE Ranking = 0 OR Name = 'Team';
DELETE FROM RECEIVING WHERE Ranking = 0 OR Name = 'Team';


DROP TABLE IF EXISTS TEAMMAP;

CREATE TABLE TEAMMAP (
TeamID VARCHAR(3), 
Team VARCHAR(50), 
Conf VARCHAR(10));

LOAD DATA INFILE 
'/mnt/disks/disk1/bebop/projects/cfbStats/data/teamMap.csv'
INTO TABLE TEAMMAP 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n';







