#!/bin/bash
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
--
-- Table structure for table urls
--
DROP TABLE IF EXISTS urls;
CREATE TABLE urls (
	id INTEGER PRIMARY KEY,
	manifest_id INTEGER,
	url TEXT DEFAULT NULL
);
!

# show tables
sqlite3 $DB '.tables'

chmod 666 $DB
