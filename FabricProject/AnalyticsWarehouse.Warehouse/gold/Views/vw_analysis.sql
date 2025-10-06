-- Auto Generated (Do not modify) 7F5878D7EA9D3EFB1D91BB30818CC18D116934CD1F326C122D6E93267A5E54B7
CREATE VIEW [gold].[vw_analysis]
AS 
WITh CTE AS(

    SELECT
     F.*,
     P.ModelName,
     P.ProductPrice,
     P.ProductName
    From gold.fact_sales F
    LEFT JOIN gold.dim_products P 
    ON F.ProductKey=P.ProductKey

    )

    SELECT
    CTE.ModelName
    ,Count(DISTINCT CTE.CustomerKey) as tot_cust 
    ,COUNT(DISTINCT CTE.TerritoryKey) as tot_territory
    FROM CTE
    GROUP BY CTE.ModelName
    HAVING COUNT(DISTINCT CTE.CustomerKey)>1000