# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table Metadata Queries
# MAGIC 
# MAGIC Using advanced metadata queries on delta tables. The data used for this notebook is available under `databricks-datasets/nyctaxi/sample/json/` in most databricks workspaces.

# COMMAND ----------

# Read in the nyctaxi data
df = (
    spark
        .read
        .format('json')
        .option('inferSchema', 'true')
        .load('/databricks-datasets/nyctaxi/sample/json/')
)

# Save output as a managed delta table
(
    df 
        .write
        .format('delta')
        .partitionBy('pep_pickup_date_txt')
        .saveAsTable('default.nyctaxi')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command `DESCRIBE HISTORY`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY default.nyctaxi

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert a new record
# MAGIC INSERT INTO default.nyctaxi
# MAGIC SELECT * FROM default.nyctaxi
# MAGIC   WHERE DOLocationID = 4 and pep_pickup_date_txt = '2019-11-30';
# MAGIC   
# MAGIC   
# MAGIC -- Delete records
# MAGIC DELETE FROM default.nyctaxi
# MAGIC   WHERE DOLocationID = 7 AND pep_pickup_date_txt = '2019-12-13';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY default.nyctaxi;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE default.nyctaxi TO VERSION AS OF 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command `DESCRIBE TABLE`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE default.nyctaxi

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED default.nyctaxi

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command `DESCRIBE DETAIL`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL default.nyctaxi

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command `SHOW CREATE TABLE`

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CREATE TABLE default.nyctaxi

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW default.nyctaxi_pickup_passenger_avg AS 
# MAGIC   SELECT 
# MAGIC     PULocationID as pickup_location_id,
# MAGIC     round(avg(passenger_count)) as avg_passenger_count
# MAGIC   FROM default.nyctaxi
# MAGIC   GROUP BY PULocationID
# MAGIC   ORDER BY avg_passenger_count DESC;
# MAGIC   
# MAGIC SHOW CREATE TABLE default.nyctaxi_pickup_passenger_avg;
