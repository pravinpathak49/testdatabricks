# Databricks notebook source
from datetime import date, timedelta
import random
from pyspark.sql import SparkSession

# Item names dictionary for each category
item_names = {
    "Electronics": ["Laptop", "Smartphone", "Tablet", "Headphones", "Smartwatch"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Hat"],
    "Grocery": ["Milk", "Bread", "Eggs", "Cheese", "Coffee"],
    "Toys": ["Action Figure", "Doll", "Puzzle", "Board Game", "Toy Car"],
    "Books": ["Novel", "Biography", "Science Book", "Cookbook", "Magazine"],
    "Sports": ["Basketball", "Soccer Ball", "Tennis Racket", "Yoga Mat", "Dumbbell"]
}

def generate_data(num_records=50, start_id=1, columns=["ID", "DATE", "CATEGORY", "ITEM_NAME", "AMOUNT"]):
    records = []
    categories = list(item_names.keys())
    start_date = date(2023, 1, 1)
    
    # Generate values for each record
    for i in range(num_records):
        record = []
        
        if "ID" in columns:
            record.append(start_id + i)  # Start from start_id
        
        if "DATE" in columns:
            record_date = start_date + timedelta(days=random.randint(0, 365))  # Random date within a year
            record.append(record_date.isoformat())
        
        if "CATEGORY" in columns:
            category = random.choice(categories)
            record.append(category)
        
        if "ITEM_NAME" in columns:
            item_name = random.choice(item_names[random.choice(categories)])  # Random item name based on category
            record.append(item_name)
        
        if "AMOUNT" in columns:
            amount = random.randint(5, 500)  # Random amount between $5 and $500
            record.append(amount)

        if "QUANTITY" in columns:
            quantity = random.randint(1, 10)  # Random quantity
            record.append(quantity)
        
        # Add the record to the list of records
        records.append(tuple(record))
    
    # Create the Spark DataFrame from the records and column names
    df = spark.createDataFrame(records, schema=columns)
    
    return df

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS retail;
# MAGIC DROP TABLE IF EXISTS retail.sales;
# MAGIC
