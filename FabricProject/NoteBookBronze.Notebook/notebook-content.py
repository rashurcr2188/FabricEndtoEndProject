# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "491162e6-65ef-43b3-9976-df40387af5a0",
# META       "default_lakehouse_name": "lakehousebronze",
# META       "default_lakehouse_workspace_id": "d303dfa9-ba89-4bdc-bb35-30e3a647275b",
# META       "known_lakehouses": [
# META         {
# META           "id": "491162e6-65ef-43b3-9976-df40387af5a0"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Data Engineering Session -  1**

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/RawDataADLS/Returns.csv")
# df now is a Spark DataFrame containing CSV data from "Files/RawDataADLS/Returns.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lakehousebronze.adlstable LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
