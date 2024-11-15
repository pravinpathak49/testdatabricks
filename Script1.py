# Databricks notebook source
# MAGIC %run ./generic_functions

# COMMAND ----------

columns = ["ID", "DATE", "CATEGORY", "ITEM_NAME", "AMOUNT"] 
raw_sales = generate_data(num_records=5, start_id=1, columns=columns)
display(raw_sales)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS retail;
# MAGIC DROP TABLE IF EXISTS retail.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in retail;

# COMMAND ----------

raw_sales.createOrReplaceTempView('tempsales')
spark.sql('select * from tempsales').show()

spark.sql("""
    CREATE OR REPLACE TABLE retail.sales
    USING DELTA
    AS SELECT * FROM tempsales
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail.sales;

# COMMAND ----------

sales_n1 = generate_data(num_records=5, start_id=6, columns=columns)
display(sales_n1)

# COMMAND ----------

sales_n1.write.mode('append').saveAsTable('retail.sales')

#what happen if we run multiple times.??
#we will get duplicate , then what is the solution??

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail.sales;

# COMMAND ----------

#lets remove duplicate.
# Load the Delta table into a DataFrame
df = spark.read.format("delta").table("retail.sales")
#drop duplicates
df_deduped = df.dropDuplicates()
# Write back to the Delta table, overwriting the existing data
df_deduped.write.format("delta").mode("overwrite").saveAsTable("retail.sales")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from retail.sales;

# COMMAND ----------

#introduce merge
sales_n2 = generate_data(num_records=5, start_id=11, columns=columns)
display(sales_n2)

# COMMAND ----------

from delta.tables import DeltaTable
delta_tbl1 = DeltaTable.forName(spark, "retail.sales")


delta_tbl1.alias("target").merge(
    sales_n2.alias("source"),
    "target.ID = source.ID "  # Match condition based on the ID column
).whenMatchedUpdateAll()\
  .whenNotMatchedInsertAll()\
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail.sales;

# COMMAND ----------

columns=["ID", "DATE", "CATEGORY", "ITEM_NAME", "AMOUNT", "QUANTITY"] 
sales_n3 = generate_data(num_records=5, start_id=16, columns=columns)
display(sales_n3)

# COMMAND ----------

from delta.tables import DeltaTable
delta_tbl1 = DeltaTable.forName(spark, "retail.sales")

delta_tbl1.alias("target").merge(
    sales_n3.alias("source"),
    "target.ID = source.ID "  # Match condition based on the ID column
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

#check if new columns is added or not.


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM retail.sales WHERE id > 15;
# MAGIC select * from retail.sales;

# COMMAND ----------

spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")

# COMMAND ----------

#set automerge schema true.
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

from delta.tables import DeltaTable
delta_tbl1 = DeltaTable.forName(spark, "retail.sales")

delta_tbl1.alias("target").merge(
    sales_n3.alias("source"),
    "target.ID = source.ID "  # Match condition based on the ID column
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail.sales;

# COMMAND ----------


