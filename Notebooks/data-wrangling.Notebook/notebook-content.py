# Fabric notebook source


# CELL ********************

%md Data Loading

# CELL ********************

%fs ls dbfs:/FileStore/shared_uploads/delta/retail_silver.delta

# CELL ********************

raw_data = spark.read.format("delta").load("dbfs:/FileStore/shared_uploads/delta/retail_silver.delta")
raw_data.where("gender is not null").display()

# CELL ********************

%md Data Selection

# CELL ********************

select_data = (raw_data
                .where("age is not null")
                .select("invoice_num", "stock_code","quantity", "invoice_date", 
                              "unit_price","country_code","age", "gender", "occupation", "work_class", "final_weight", "description"))
select_data.display()

# CELL ********************

%md Calculating basic Statistics

# CELL ********************

select_data.describe().display()

# CELL ********************

%md Data Cleansing

# CELL ********************

dedupe_data = select_data.drop_duplicates(["invoice_num", "invoice_date", "stock_code"])
dedupe_data.display()

# CELL ********************

%md Change data types

# CELL ********************

from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import FloatType
interim_data = (select_data
                  .withColumn("invoice_time", to_timestamp("invoice_date", 'dd/M/yy HH:mm'))
                  .withColumn("cust_age", col("age").cast(FloatType()))
                  .withColumn("working_class", col("work_class").cast(FloatType()))
                  .withColumn("fin_wt", col("final_weight").cast(FloatType()))
                  .drop("final_weight")
             )
interim_data.display()

# CELL ********************

%md Replace NULL and NA values

# CELL ********************

clean_data = interim_data.na.fill(0)
clean_data.display()

# CELL ********************

%md Data Manipulation

# CELL ********************

final_data = (clean_data.where("age is not null")
                       .withColumnRenamed("working_class", "work_type")
                       .drop("age")
                       .drop("work_class")
                       .drop("fn_wt")
             )
final_data.display()

# CELL ********************

#%fs rm -r /FileStore/shared_uploads/delta/retail_ml.delta

# CELL ********************

(final_data.where("description is NOT NULL")
       .write.format("delta").mode("overwrite")
         .save("dbfs:/FileStore/shared_uploads/delta/retail_ml.delta"))

# CELL ********************

final_data.count()

# CELL ********************

%md Convert Spark DataFrame to a Pandas Dataframe

# CELL ********************

pd_data = final_data.toPandas()
pd_data
