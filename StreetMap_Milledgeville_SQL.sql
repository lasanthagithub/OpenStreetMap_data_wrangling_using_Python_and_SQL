## Creating a database to store tables
CREATE SCHEMA Milledgeville_dbs;
USE Milledgeville_dbs;

## Creating empty tables 
CREATE TABLE nodes (
	changeset INT NOT NULL,    
	nod_id DOUBLE NOT NULL,
    user_nm VARCHAR(50) NOT NULL,
    uid INT NOT NULL,    
    lon DECIMAL(30, 26) NOT NULL,
    lat DECIMAL(30, 26) NOT NULL,
    PRIMARY KEY (nod_id)
    );


CREATE TABLE node_tags (
	k VARCHAR(50) NOT NULL, 	
    v VARCHAR(200) NOT NULL,
    nod_id INT4 NOT NULL    
    );
#primary key (nod_id)


CREATE TABLE ways (
	changeset INT NOT NULL,
    user_nm VARCHAR(50) NOT NULL,     
    uid INT NOT NULL,
    way_id INT4 NOT NULL  
    
    );


CREATE TABLE way_tags (
	k VARCHAR(50) NOT NULL, 	
    v VARCHAR(200) NOT NULL,
    way_id INT4 NOT NULL    
    );
    

CREATE TABLE way_nodes (
	ref INT4 NOT NULL,
    way_id INT4 NOT NULL
    
);


## Load the data to the table from .csv files
LOAD DATA LOCAL INFILE 'F:/One_drive_Gmail/OneDrive/Dropbox_transfers/Udacity/Nano_Degree_Data_analysis/Project_4_Data_wrangling/Final_project/nodes.csv'
INTO TABLE nodes
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'F:/One_drive_Gmail/OneDrive/Dropbox_transfers/Udacity/Nano_Degree_Data_analysis/Project_4_Data_wrangling/Final_project/node_tags.csv'
INTO TABLE node_tags
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'F:/One_drive_Gmail/OneDrive/Dropbox_transfers/Udacity/Nano_Degree_Data_analysis/Project_4_Data_wrangling/Final_project/ways.csv'
INTO TABLE ways
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'F:/One_drive_Gmail/OneDrive/Dropbox_transfers/Udacity/Nano_Degree_Data_analysis/Project_4_Data_wrangling/Final_project/way_tags.csv'
INTO TABLE way_tags
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'F:/One_drive_Gmail/OneDrive/Dropbox_transfers/Udacity/Nano_Degree_Data_analysis/Project_4_Data_wrangling/Final_project/way_nodes.csv'
INTO TABLE way_nodes
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

## dropping tables if nessary
#DROP TABLE node_tags;
#DROP TABLE milledgeville_dbs.nodes;
#DROP TABLE ways;
#DROP TABLE way_nodes;
#DROP TABLE way_tags;


SELECT v, k
FROM node_tags
WHERE k = 'addr:street';

## Working with zipcodes
SELECT v, COUNT(v) AS zipcodes
FROM node_tags
WHERE k LIKE '%post%'
GROUP BY v
ORDER BY zipcodes DESC;

SELECT v, COUNT(v) AS zipcodes
FROM way_tags
WHERE k LIKE '%post%'
GROUP BY v
ORDER BY zipcodes DESC;

## Unique users



## Top 10 zip codes
SELECT (v) as Zipcode, COUNT(v) AS counts
FROM (SELECT v 
	FROM node_tags
	WHERE k LIKE '%post%'
	UNION ALL
	SELECT v
	FROM way_tags
	WHERE k LIKE '%post%') zipcds
GROUP BY Zipcode
ORDER BY counts DESC
LIMIT 10;


## Inappropriate zip codess
SELECT (v) as Zipcode, COUNT(v) AS counts
FROM (SELECT v 
	FROM node_tags
	WHERE k LIKE '%post%' and v not like '%3%'
	UNION ALL
	SELECT v
	FROM way_tags
	WHERE k LIKE '%post%'  and v not like '%3%') zipcds
GROUP BY Zipcode;


## User count for nodes
SELECT user_nm, COUNT(user_nm) AS user_count
FROM nodes
GROUP BY user_nm
ORDER BY user_count DESC
LIMIT 15;

## User count for ways
SELECT user_nm, COUNT(user_nm) AS user_count
FROM ways
GROUP BY user_nm
ORDER BY user_count DESC
LIMIT 15;

## Number of nodes
SELECT COUNT(user_nm) AS way_count
FROM nodes;


## Number of way nodes
SELECT COUNT(user_nm) AS way_count
FROM ways;

## Select elementary schools in the area
SELECT v as Elemtry_School
FROM node_tags
WHERE k = 'name' AND v LIKE '%Elementary School%'
ORDER BY Elemtry_School; 


## User count for both ways and nodes
SELECT user_nm, SUM(usr_cunt) AS user_count
FROM 	(SELECT user_nm, COUNT(user_nm) AS usr_cunt 
		FROM ways 
        GROUP BY user_nm
        UNION ALL 
		SELECT user_nm, COUNT(user_nm) AS usr_cunt 
        FROM nodes
        GROUP BY user_nm)  ways_nodes
GROUP BY user_nm
ORDER BY user_count DESC
LIMIT 20; 


## User count for both ways and nodes and save into a table
CREATE TABLE user_summary AS 
SELECT user_nm, SUM(usr_cunt) AS user_count 
FROM 	(SELECT user_nm,COUNT(user_nm) AS usr_cunt 
		FROM ways 
        GROUP BY user_nm
        UNION ALL 
		SELECT user_nm, COUNT(user_nm) AS usr_cunt 
        FROM nodes
        GROUP BY user_nm)  ways_nodes
GROUP BY user_nm
ORDER BY user_count DESC;

## User count and percentage for both ways and nodes
SELECT user_nm, user_count, (user_count/tot)*100 AS percentage
FROM user_summary 
CROSS JOIN (SELECT SUM(user_count) AS tot
			FROM user_summary) total
LIMIT 20;

## Unique user count
SELECT COUNT(username) as usercount 
FROM (SELECT DISTINCT(user_nm) as username
	FROM user_summary) usecont;
