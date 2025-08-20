# Adani Stock Market SQL Analysis

This project explores **Adani Group stocks** using SQL for **ETL, transformation, validation, and analysis**.
We load raw stock market data (CSV), clean and transform it into a structured format, and run queries to uncover insights such as returns, volumes, and price trends.

---
# Dataset Information  

This project uses the **Adani Group Stocks Dataset** sourced from Kaggle.  
It contains historical daily trading data for multiple Adani Group companies.  

**Columns in the dataset:**  
- `trade_timestamp` → Raw timestamp in nanoseconds since epoch  
- `symbol` → Stock ticker symbol  
- `company` → Full company name  
- `open_price` → Opening price of the stock  
- `high_price` → Highest trading price during the day  
- `low_price` → Lowest trading price during the day  
- `close_price` → Closing price of the stock  
- `volume` → Total number of shares traded  
- `dividends` → Dividend issued (if any)  
- `stock_splits` → Stock split ratio (if any)  

 Covers **all major Adani businesses** listed on the Indian stock market.  
 Data is at **daily frequency** and can be aggregated to monthly/yearly levels.  

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

##  2. Data Transformation

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

##  3. ETL Validation Checks

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

#  Data Exploration & Analysis on Adani Stock Dataset

After cleaning and preparing the data, we perform exploratory queries to uncover trading trends, stock performance, and investor behavior across Adani Group companies.



##  Q1. Total Records in the Dataset
```sql
SELECT COUNT(*) AS TotalRecords
FROM AdaniStockData_Clean;
````

Tells us **how many trading entries** exist in the dataset.

---

##  Q2. Distinct Companies & Symbols

```sql
SELECT DISTINCT company, symbol 
FROM AdaniStockData_Clean;
```

 -Helps verify **all companies included** and their ticker symbols.

---

##  Q3. Top 5 Companies by Highest Trading Volumes

```sql
SELECT TOP 5 company, 
       SUM(volume) AS HighestVolumeTraded 
FROM AdaniStockData_Clean
GROUP BY company 
ORDER BY SUM(volume) DESC;
```

 -Identifies the **most actively traded Adani companies**.

---

##  Q4. Top 5 Companies by Lowest Trading Volumes

```sql
SELECT TOP 5 company, 
       SUM(volume) AS LowestVolumeTraded 
FROM AdaniStockData_Clean
GROUP BY company 
ORDER BY SUM(volume) ASC;
```

 -Highlights **less active or less popular stocks** in the group.

---

##  Q5. Yearly Average Open & Close Price per Company

```sql
SELECT 
    company,
    YEAR(trade_date) AS TradeYear,
    ROUND(AVG(open_price), 3) AS AvgOpenPrice, 
    ROUND(AVG(close_price), 3) AS AvgClosePrice
FROM AdaniStockData_Clean
GROUP BY company, YEAR(trade_date)
ORDER BY company, TradeYear;
```

 -Shows **yearly price fluctuations**, helping identify long-term trends.

---

##  Q6. Monthly & Yearly Returns (Cumulative)

```sql
WITH DailyReturns AS (
    SELECT 
        company,
        symbol,
        trade_date,
        YEAR(trade_date) AS TradeYear,
        MONTH(trade_date) AS TradeMonth,
        ROUND(((close_price - open_price) / open_price) * 100, 3) AS DailyReturn
    FROM AdaniStockData_Clean
    WHERE open_price > 0
),
MonthlyReturns AS (
    SELECT
        company,
        symbol,
        TradeYear,
        TradeMonth,
        SUM(DailyReturn) AS MonthlyReturn
    FROM DailyReturns
    GROUP BY company, symbol, TradeYear, TradeMonth
)
SELECT
    company,
    symbol,
    TradeYear,
    TradeMonth,
    MonthlyReturn,
    SUM(MonthlyReturn) OVER (
        PARTITION BY company, symbol, TradeYear
        ORDER BY TradeMonth
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS YearlyReturnRunning
FROM MonthlyReturns
ORDER BY company, TradeYear, TradeMonth;
```

 -Converts **daily price movements → monthly trends → yearly cumulative returns**.
 -Helps answer: *“Which stocks are winners, losers, consistent, volatile, or seasonal?”*

---

##  Q7. Monthly & Yearly Average Trading Volumes

```sql
WITH MonthlyAvgVolumes AS (
    SELECT
        company,
        symbol,
        YEAR(trade_date) AS TradeYear,
        MONTH(trade_date) AS TradeMonth,
        ROUND(AVG(volume), 2) AS MonthlyAvgVolume
    FROM AdaniStockData_Clean
    GROUP BY company, symbol, YEAR(trade_date), MONTH(trade_date)
)
SELECT
    company,
    symbol,
    TradeYear,
    TradeMonth,
    MonthlyAvgVolume,
    ROUND(
        AVG(MonthlyAvgVolume) OVER (
            PARTITION BY company, symbol, TradeYear
            ORDER BY TradeMonth
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2
    ) AS YearlyAvgVolumeRunning
FROM MonthlyAvgVolumes
ORDER BY company, TradeYear, TradeMonth;
```

 -Tracks **trading interest over time**.
- Reveals seasonal/structural shifts & events impacting market activity.

---

##  Q8. Dividends Issued by Companies

```sql
SELECT company, 
       SUM(dividends) AS TotalDividends
FROM AdaniStockData_Clean
GROUP BY company
HAVING SUM(dividends) > 0;
```

 -Identifies **dividend-paying companies** and their total payouts.

---

## Q9. Stock Splits by Company

```sql
SELECT company, 
       COUNT(stock_splits) AS TotalSplits
FROM AdaniStockData_Clean
WHERE stock_splits > 0
GROUP BY company;
```

 -Tracks **corporate actions** (stock splits) that affect price & liquidity.

---

## Q10. Monthly & Yearly Price Change (First vs Last Close)

```sql
WITH MonthlyPriceChange AS (
    SELECT 
        company,
        symbol,
        YEAR(trade_date) AS TradeYear,
        MONTH(trade_date) AS TradeMonth,
        FIRST_VALUE(close_price) OVER (
            PARTITION BY company, YEAR(trade_date), MONTH(trade_date)
            ORDER BY trade_date ASC
        ) AS FirstClose,
        LAST_VALUE(close_price) OVER (
            PARTITION BY company, YEAR(trade_date), MONTH(trade_date)
            ORDER BY trade_date ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS LastClose
    FROM AdaniStockData_Clean
),
MonthlyReturn AS (
    SELECT DISTINCT
        company,
        symbol,
        TradeYear,
        TradeMonth,
        ROUND(LastClose - FirstClose, 3) AS MonthlyPriceChange
    FROM MonthlyPriceChange
)
SELECT
    company,
    symbol,
    TradeYear,
    TradeMonth,
    MonthlyPriceChange,
    AVG(MonthlyPriceChange) OVER (
        PARTITION BY company, symbol, TradeYear
        ORDER BY TradeMonth
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS YearlyPriceChangeRunning
FROM MonthlyReturn
ORDER BY company, TradeYear, TradeMonth;
```

 Measures **stock momentum** by comparing earliest vs latest closing prices.
- Smooths out daily noise → provides a **clearer picture of long-term trend direction**.




