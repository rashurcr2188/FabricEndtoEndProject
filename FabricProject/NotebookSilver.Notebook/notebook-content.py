# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "10842aa6-812f-4a20-a3b5-141b0de7ca76",
# META       "default_lakehouse_name": "SilverLakeHouse",
# META       "default_lakehouse_workspace_id": "d303dfa9-ba89-4bdc-bb35-30e3a647275b",
# META       "known_lakehouses": [
# META         {
# META           "id": "10842aa6-812f-4a20-a3b5-141b0de7ca76"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Reading Shortcut**

# CELL ********************

from pyspark.sql.functions import*
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/RawDataAdventureWorks_Customers/AdventureWorks_Customers.csv")
# df now is a Spark DataFrame containing CSV data from "Files/RawDataAdventureWorks_Customers/AdventureWorks_Customers.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reading Bronze LAkehouse Data**

# MARKDOWN ********************

# **Reading Customers Data**

# CELL ********************

df=df.withColumn('FullName',concat(df.Prefix,lit(" "),df.FirstName,lit(" "),df.LastName))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=df.withColumn("doamin",split(df.EmailAddress,"@")[1])
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write the data in parquet delta format**

# CELL ********************

df.write.format("delta")\
    .mode("append")\
    .option("path","Files/Customers")\
    .saveAsTable("customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Sales data union**

# CELL ********************

df_sales2016 = spark.read.format("csv").option("header","true")\
                .load("Files/RawDataAdventureWorks_Sales_2016/AdventureWorks_Sales_2016.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales2017 = spark.read.format("csv").option("header","true")\
                .load("Files/RawDataAdventureWorks_Sales_2017/AdventureWorks_Sales_2017.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales=df_sales2016.union(df_sales2017)
display(df_sales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales.write.format("delta")\
              .mode("append")\
              .option("path","Files/Sales")\
              .saveAsTable("sales")  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SilverLakeHouse.sales LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reading Cat and Subcat data **

# CELL ********************

df_subcat=spark.read.format("delta")\
                .load("abfss://FabricWorkspacerashu@onelake.dfs.fabric.microsoft.com/lakehousebronze.Lakehouse/Tables/Merge")

display(df_subcat)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_subcat.write.format("delta")\
              .mode("append")\
              .option("path","Files/subcat")\
              .saveAsTable("subcat")  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Cal = spark.read.format("csv").option("header","true")\
                .load("abfss://FabricWorkspacerashu@onelake.dfs.fabric.microsoft.com/lakehousebronze.Lakehouse/Files/RawDataAdventureWorks_Calendar")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Cal=df_Cal.withColumn("MONTH",split("Date","/")[0])
df_Cal=df_Cal.withColumn("DAY",split("Date","/")[1])
df_Cal=df_Cal.withColumn("YEAR",split("Date","/")[2])

display(df_Cal)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Cal.write.format("delta")\
        .mode("append")\
        .option("path","Files/calendar")\
        .saveAsTable("calendar")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Read products data file**

# CELL ********************

df_pdts = spark.read.format("csv").option("header","true")\
                .load("abfss://FabricWorkspacerashu@onelake.dfs.fabric.microsoft.com/lakehousebronze.Lakehouse/Files/RawDataAdventureWorks_Products/AdventureWorks_Products.csv")


display(df_pdts)               

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pdts=df_pdts.withColumn("ProductSKU",split("ProductSKU","-")[0])
display(df_pdts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pdts.write.format("delta")\
        .mode("append")\
        .option("path","Files/products")\
        .saveAsTable("products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Read Returns.csv file**

# CELL ********************

df_ret = spark.read.format("csv").option("header","true")\
                .load("abfss://FabricWorkspacerashu@onelake.dfs.fabric.microsoft.com/lakehousebronze.Lakehouse/Files/RawDataADLS/Returns.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_ret)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ret.write.format("csv")\
        .mode("append")\
        .option("path","Files/returns")\
        .saveAsTable("returns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.ls("Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.mkdirs("Files/mkdir")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.cp("Files/calendar","Files/mmkdir",True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_subcat)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_subcat=df_subcat.drop("ProductCategoryKey.1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_subcat.write.format("delta")\
            .mode("append")\
            .option("path","Files/subcat")\
            .saveAsTable("subcat")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_subcat.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_subcat)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_subcat.write.format("delta")\
            .mode("append")\
            .option("path","Files/subcat")\
            .saveAsTable("subcat")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
