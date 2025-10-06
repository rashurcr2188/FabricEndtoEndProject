
CREATE TABLE AnalyticsWarehouse.gold.fact_sales
AS
SElect * from SilverLakeHouse.dbo.sales_man


CREATE TABLE AnalyticsWarehouse.gold.dim_customers
AS
SElect * from SilverLakeHouse.dbo.customers_man

CREATE TABLE AnalyticsWarehouse.gold.dim_products
AS
SElect * from SilverLakeHouse.dbo.products_man

CREATE TABLE AnalyticsWarehouse.gold.dim_calendar
AS
SElect * from SilverLakeHouse.dbo.calendar_man
