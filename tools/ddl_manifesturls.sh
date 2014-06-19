#!/bin/bash
#Thu Jun  5 12:36:04 EDT 2014
set -x

#Create ewmigration database
DB='../files/ewmigration.db'
sqlite3 $DB <<!
--
-- Table structure for table manifesturl
--
DROP TABLE IF EXISTS manifesturls;
CREATE TABLE manifesturls (
	id INTEGER PRIMARY KEY,
	manifesturl TEXT DEFAULT NULL,
	timestamp TEXT DEFAULT NULL,
	email TEXT DEFAULT NULL, 
	description TEXT DEFAULT NULL 
);
!

# show tables
sqlite3 $DB '.tables'

chmod 666 $DB
