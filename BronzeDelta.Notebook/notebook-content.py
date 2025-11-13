# Fabric notebook source


# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## Bronze Delta
# 
# New notebook

# In[1]:


df = spark.read.format("csv").option("header","false").load("Files/bronze/*.csv")
# df now is a Spark DataFrame containing CSV data from "Files/bronze/2019.csv".
display(df)


# **Schema on read**

# In[2]:


from pyspark.sql.types import *


# In[3]:


# Create the schema for the table
orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
    ])


# In[4]:


df = spark.read.format("csv").option("header","true").schema(orderSchema).load("Files/bronze/*.csv")
# df now is a Spark DataFrame containing CSV data from "Files/bronze/2019.csv".
display(df)


# In[5]:


df.printSchema()


# **Create Delta table using Delta APIs and insert data into table using Delta merge transaction statement**

# In[6]:


from delta.tables import *


# In[7]:


DeltaTable.createIfNotExists(spark) \
.tableName("BRONZE.sales") \
    .addColumn("SalesOrderNumber", StringType()) \
    .addColumn("SalesOrderLineNumber", IntegerType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email", StringType()) \
    .addColumn("Item", StringType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .execute()


# In[8]:


# Update existing records and insert new ones based on a condition defined by the columns SalesOrderNumber, OrderDate, CustomerName, and Item.

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/BRONZE/sales')
    
dfUpdates = df
    
deltaTable.alias('bronze') \
  .merge(
    dfUpdates.alias('updates'),
    'bronze.SalesOrderNumber = updates.SalesOrderNumber and bronze.OrderDate = updates.OrderDate and bronze.CustomerName = updates.CustomerName and bronze.Item = updates.Item'
  ) \
   .whenMatchedUpdate(set =
    {
          #Do nothing no overwriting
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "SalesOrderNumber": "updates.SalesOrderNumber",
      "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
      "OrderDate": "updates.OrderDate",
      "CustomerName": "updates.CustomerName",
      "Email": "updates.Email",
      "Item": "updates.Item",
      "Quantity": "updates.Quantity",
      "UnitPrice": "updates.UnitPrice",
      "Tax": "updates.Tax"
    }
  ) \
  .execute()

