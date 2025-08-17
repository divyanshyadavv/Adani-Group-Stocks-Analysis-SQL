------------------------------------------------------------
-- Data Exploration & Analysis on Adani Stock Dataset
------------------------------------------------------------

-- Q1. How many records are in the dataset?
SELECT COUNT(*) AS TotalRecords
FROM AdaniStockData_Clean;


-- Q2. What are the distinct companies and their symbols in the dataset?
SELECT DISTINCT company, symbol 
FROM AdaniStockData_Clean;


-- Q3. Which are the Top 5 companies with the highest trading volumes?
SELECT TOP 5 company, 
       SUM(volume) AS HighestVolumeTraded 
FROM AdaniStockData_Clean
GROUP BY company 
ORDER BY SUM(volume) DESC;


-- Q4. Which are the Top 5 companies with the lowest trading volumes?
SELECT TOP 5 company, 
       SUM(volume) AS LowestVolumeTraded 
FROM AdaniStockData_Clean
GROUP BY company 
ORDER BY SUM(volume) ASC;


-- Q5. What is the average open and close price for each company year-wise?
-- It shows yearly price fluctuations.
SELECT 
    company,
    YEAR(trade_date) AS TradeYear,
    ROUND(AVG(open_price), 3) AS AvgOpenPrice, 
    ROUND(AVG(close_price), 3) AS AvgClosePrice
FROM AdaniStockData_Clean
GROUP BY company, YEAR(trade_date)
ORDER BY company, TradeYear;


-- Q6. What is the monthly and yearly return based on the daily returns for each company?
-- This query summarizes price performance into trends, 
-- helping you answer “Which stocks are winners, losers, consistent, volatile, seasonal?”
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




-- Q7. On which date did each company record its highest trading volume?
-- this analysis doesn’t just show how much a stock trades, but also:
-- Where interest is growing/declining
-- Seasonal/structural patterns
-- Market events’ impact
-- Relative popularity among Adani group companies

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



-- Q8. Were any dividends issued by the companies? If yes, how much in total?
SELECT company, 
       SUM(dividends) AS TotalDividends
FROM AdaniStockData_Clean
GROUP BY company
HAVING SUM(dividends) > 0;


-- Q9. Did any stock splits happen? If yes, how many for each company?
SELECT company, 
       COUNT(stock_splits) AS TotalSplits
FROM AdaniStockData_Clean
WHERE stock_splits > 0
GROUP BY company;


-- Q10. How did each company's closing price change from the earliest to the latest record?
-- this query gives a clearer picture of stock momentum and trend direction, 
-- which daily data often hides because of noise.

-- Step 1: Get monthly first and last close price for each company
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
)
, MonthlyReturn AS (
    SELECT DISTINCT
        company,
        symbol,
        TradeYear,
        TradeMonth,
        ROUND(LastClose - FirstClose, 3) AS MonthlyPriceChange
    FROM MonthlyPriceChange
)
-- Step 2: Compute yearly cumulative change from monthly
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

