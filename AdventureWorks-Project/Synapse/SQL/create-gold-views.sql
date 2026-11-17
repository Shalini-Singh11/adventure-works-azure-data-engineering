CREATE VIEW gold.cal
AS
select *
from 
OPENROWSET(
    BULK 'https://adventureworkdatalake1.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT = 'PARQUET'
) as Gold_AdventureWorks_Cal

------------------------------------------
--create view customer ---------------------
------------------------------------------
CREATE VIEW gold.customer
AS
select *
from 
OPENROWSET(
    BULK 'https://adventureworkdatalake1.dfs.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT = 'PARQUET'
) as Gold_AdventureWorks_Customers

------------------------------------------
--create view Product Categories ---------------------
------------------------------------------
CREATE VIEW gold.productCategories
AS
select *
from 
OPENROWSET(
    BULK 'https://adventureworkdatalake1.dfs.core.windows.net/silver/AdventureWorks_Product_Categories/',
    FORMAT = 'PARQUET'
) as Gold_AdventureWorks_Product_Categories

------------------------------------------
--create view Product Subcategories ---------------------
------------------------------------------
CREATE VIEW gold.productSubCategories
AS
select *
from 
OPENROWSET(
    BULK 'https://adventureworkdatalake1.dfs.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
    FORMAT = 'PARQUET'
) as Gold_AdventureWorks_Product_Subcategories

------------------------------------------
--create view Product ---------------------
------------------------------------------
CREATE VIEW gold.product
AS
select *
from 
OPENROWSET(
    BULK 'https://adventureworkdatalake1.dfs.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT = 'PARQUET'
) as Gold_AdventureWorks_Product

------------------------------------------
--create view Returns ---------------------
------------------------------------------
CREATE VIEW gold.proreturns
AS
select *
from 
OPENROWSET(
    BULK 'https://adventureworkdatalake1.dfs.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT = 'PARQUET'
) as Gold_AdventureWorks_Returns

------------------------------------------
--create view Sales ---------------------
------------------------------------------
CREATE VIEW gold.sales
AS
select *
from 
OPENROWSET(
    BULK 'https://adventureworkdatalake1.dfs.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT = 'PARQUET'
) as Gold_AdventureWorks_Sales

------------------------------------------
--create view Territories ---------------------
------------------------------------------
CREATE VIEW gold.territories
AS
select *
from 
OPENROWSET(
    BULK 'https://adventureworkdatalake1.dfs.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
) as Gold_AdventureWorks_Territories