#!/bin/bash
#Thu Jun  5 12:36:35 EDT 2014
set -x

#Create ewmigration database
DB='../files/ewmigration.db'
sqlite3 $DB <<!
--
-- Table structure for table manifest
--
DROP TABLE IF EXISTS manifest;
CREATE TABLE manifest (
	id INTEGER PRIMARY KEY,
	manfname TEXT DEFAULT NULL,
	outfname TEXT DEFAULT NULL,
	email TEXT DEFAULT NULL,
	timestamp TEXT DEFAULT NULL,
	description TEXT DEFAULT NULL
);
!

# show tables
sqlite3 $DB '.tables'

chmod 666 $DB
