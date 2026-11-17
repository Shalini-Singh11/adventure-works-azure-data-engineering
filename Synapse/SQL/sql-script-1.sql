CREATE DATABASE SCOPED CREDENTIAL cred_adventureworks
WITH IDENTITY = 'Managed Identity'

-- =====================================
-- ================= Silver
-- =====================================

CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
LOCATION = 'https://adventureworkdatalake1.dfs.core.windows.net/silver',
CREDENTIAL = cred_adventureworks
)

-- =====================================
-- ================= Gold
-- =====================================

CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
LOCATION = 'https://adventureworkdatalake1.dfs.core.windows.net/gold',
CREDENTIAL = cred_adventureworks
)

-- =====================================
-- ================= External File Formate
-- =====================================

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)



-- =====================================
-- ================= Create External Table
-- =====================================

-- ================= Sales Table ======================

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.sales

-- ================= Calendar Table ======================

CREATE EXTERNAL TABLE gold.extcalender
WITH
(
    LOCATION = 'extcalender',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.cal

-- ================= Customer Table ======================

CREATE EXTERNAL TABLE gold.extcustomer
WITH
(
    LOCATION = 'extcustomer',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.customer

-- ================= Product Table ======================

CREATE EXTERNAL TABLE gold.extproduct
WITH
(
    LOCATION = 'extproduct',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.product

-- ================= ProductCategories Table ======================

CREATE EXTERNAL TABLE gold.extproductCategories
WITH
(
    LOCATION = 'extproductCategories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.productCategories

-- ================= ProductSubCategories Table ======================

CREATE EXTERNAL TABLE gold.extproductSubCategories
WITH
(
    LOCATION = 'extproductSubCategories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.productSubCategories

-- ================= Product Returns Table ======================

CREATE EXTERNAL TABLE gold.extproreturns
WITH
(
    LOCATION = 'extproreturns',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.proreturns

-- ================= Territories Table ======================

CREATE EXTERNAL TABLE gold.extterritories
WITH
(
    LOCATION = 'extterritories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.territories