-- SiteMerge Database Preparation Script
-- This script creates the sitemerge database if it doesn't exist
-- All project tables will be created in this database

-- Create the sitemerge database
CREATE DATABASE IF NOT EXISTS sitemerge 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

-- Show the created database
SHOW DATABASES LIKE 'sitemerge';

-- Use the sitemerge database
USE sitemerge;

-- Display current database
SELECT DATABASE() AS current_database; 