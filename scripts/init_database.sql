/*
==============================================
Create Database and Schemas
==============================================

This script creates a new database named 'DataWarehouse' after checking it is already exists.
If the database exists, it will be dropped and recreated.
It also creates three schemas: 'bronze', 'silver', and 'gold'.

WARNING
This script will drop the existing 'DataWarehouse' database if it exists, which will result in loss of all data in that database.
*/


USE master;
GO

--Drop and recreate 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO