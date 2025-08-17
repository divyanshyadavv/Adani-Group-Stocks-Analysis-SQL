# 📊 Adani Stock Market SQL Analysis

This project explores **Adani Group stocks** using SQL for **ETL, transformation, validation, and analysis**.
We load raw stock market data (CSV), clean and transform it into a structured format, and run queries to uncover insights such as returns, volumes, and price trends.

---

## ⚙️ 1. Database Setup

* **Database Creation**: A dedicated database `AdaniDB` is created.
* **Raw Table Creation**: A staging table `AdaniStockData` is defined to store raw stock data.

```sql
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
```

---

## 🔄 2. Data Transformation

* Convert **nanosecond timestamps → human-readable datetime**
* Fix **invalid prices** (replace negative values with `0`)
* Handle **missing values** (replace `NULL` with `0` for dividends & stock\_splits)

```sql
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
    CASE WHEN open_price < 0 THEN 0 ELSE open_price END AS open_price,
    CASE WHEN high_price < 0 THEN 0 ELSE high_price END AS high_price,
    CASE WHEN low_price < 0 THEN 0 ELSE low_price END AS low_price,
    CASE WHEN close_price < 0 THEN 0 ELSE close_price END AS close_price,
    volume,
    ISNULL(dividends,0) AS dividends,
    ISNULL(stock_splits,0) AS stock_splits
FROM dbo.AdaniStockData;
GO
```

---

## ✅ 3. ETL Validation Checks

* **Row Count**: Ensures data loaded correctly.
* **Unique Companies**: Confirms distinct symbols.
* **Date Range**: Validates time span of stock data.

```sql
-- Count records
SELECT COUNT(*) AS TotalRows FROM dbo.AdaniStockData_Clean;

-- Check distinct companies
SELECT DISTINCT company, symbol FROM dbo.AdaniStockData_Clean;

-- Check min and max dates
SELECT MIN(trade_date) AS StartDate, MAX(trade_date) AS EndDate
FROM dbo.AdaniStockData_Clean;
GO
```

---

👉 Next sections will include **Analytical Queries** (returns, volumes, yearly trends, and price comparisons), with insights and possible **visualization ideas** for Tableau/Power BI.

---

Do you want me to **continue the README and add the analysis queries** (like daily/monthly/yearly returns, volumes, price change) in the same structured style with explanations for each?
