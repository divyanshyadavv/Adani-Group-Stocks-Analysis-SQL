-- =========================================================
-- Project: Adani Stock Market SQL Analysis
-- Step 1: ETL - Load Raw Data into SQL Server
-- =========================================================

-- Create and use database
IF DB_ID('AdaniDB') IS NULL
    CREATE DATABASE AdaniDB;
GO
USE AdaniDB;
GO

-- Drop existing table if re-running ETL
IF OBJECT_ID('dbo.AdaniStockData', 'U') IS NOT NULL
    DROP TABLE dbo.AdaniStockData;
GO

-- Create Raw Table
CREATE TABLE dbo.AdaniStockData (
    trade_timestamp BIGINT,       -- raw timestamp in nanoseconds
    symbol VARCHAR(50),
    company VARCHAR(255),
    open_price DECIMAL(18,3),
    high_price DECIMAL(18,3),
    low_price DECIMAL(18,3),
    close_price DECIMAL(18,3),
    volume BIGINT,
    dividends DECIMAL(18,3),
    stock_splits DECIMAL(18,3)
);
GO

-- Bulk Insert Raw CSV
BULK INSERT dbo.AdaniStockData
FROM 'C:\Users\Dell\adani_stocks_project\adani.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
GO
=========================================================
-- Step 2: Data Transformation
-- - Retain raw nanosecond timestamps
-- - Convert into proper datetime
-- - Clean missing/invalid values if any
=========================================================

-- Create a clean staging table
IF OBJECT_ID('dbo.AdaniStockData_Clean', 'U') IS NOT NULL
    DROP TABLE dbo.AdaniStockData_Clean;
GO

CREATE TABLE dbo.AdaniStockData_Clean (
    trade_timestamp BIGINT,        -- raw timestamp (ns since epoch)
    trade_date DATETIME,           -- converted timestamp
    symbol VARCHAR(50),
    company VARCHAR(255),
    open_price DECIMAL(18,3),
    high_price DECIMAL(18,3),
    low_price DECIMAL(18,3),
    close_price DECIMAL(18,3),
    volume BIGINT,
    dividends DECIMAL(18,3),
    stock_splits DECIMAL(18,3)
);
GO

-- Insert transformed data
INSERT INTO dbo.AdaniStockData_Clean
SELECT 
    trade_timestamp, 
    DATEADD(SECOND, trade_timestamp / 1000000000, '1970-01-01') AS trade_date, -- ns → datetime
    symbol,
    company,
    CASE WHEN open_price < 0 THEN 0 ELSE open_price END AS open_price,   -- fix invalid prices
    CASE WHEN high_price < 0 THEN 0 ELSE high_price END AS high_price,
    CASE WHEN low_price < 0 THEN 0 ELSE low_price END AS low_price,
    CASE WHEN close_price < 0 THEN 0 ELSE close_price END AS close_price,
    volume,
    ISNULL(dividends,0) AS dividends,          -- handle missing dividends
    ISNULL(stock_splits,0) AS stock_splits     -- handle missing stock splits
FROM dbo.AdaniStockData;
GO


-- =========================================================
-- Step 3: ETL Validation Checks
-- =========================================================

-- Count records
SELECT COUNT(*) AS TotalRows FROM dbo.AdaniStockData_Clean;

-- Check distinct companies
SELECT DISTINCT company, symbol FROM dbo.AdaniStockData_Clean;

-- Check min and max dates
SELECT MIN(trade_date) AS StartDate, MAX(trade_date) AS EndDate
FROM dbo.AdaniStockData_Clean;
GO
