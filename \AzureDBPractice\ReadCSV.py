# Databricks notebook source
#Read Data from CSV Practice from : https://docs.microsoft.com/en-us/learn/modules/read-write-data-azure-databricks/2-read-data-csv-format


# COMMAND ----------

# Configure classroom
%run "/Users/piyush.u.kumar@avanade.com/03-Reading-and-writing-data-in-Azure-Databricks/Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %run "/Users/piyush.u.kumar@avanade.com/03-Reading-and-writing-data-in-Azure-Databricks/Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %run "/Users/piyush.u.kumar@avanade.com/03-Reading-and-writing-data-in-Azure-Databricks/Includes/Utility-Methods"

# COMMAND ----------

#entry point
print(spark)

# COMMAND ----------

# MAGIC %fs ls /mnt/training/wikipedia/pageviews

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv

# COMMAND ----------

#Read the CSV Files 
csvFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"
tempDF = (spark.read.option("sep", "\t")
         .csv(csvFile))

# COMMAND ----------

tempDF.printSchema()

# COMMAND ----------

display(tempDF)

# COMMAND ----------

#Step 2 - Use the File's Header
(spark.read                    
   .option("sep", "\t")        
   .option("header", "true")  
   .csv(csvFile)               
   .printSchema()
)


# COMMAND ----------

tempDF.printSchema()

# COMMAND ----------

#Step 3-  Infer the Schema
(spark.read
  .option("header", "true")
  .option("sep", "\t")
  .option("inferSchema","true")
   .csv(csvFile)
   .printSchema()
)

# COMMAND ----------

#Reading With User-Defined Schema
#Step 1 - Define The Schema
from pyspark.sql.types import *

csvSchema = StructType([
  StructField("timestamp", StringType(), False),
  StructField("site", StringType(), False),
  StructField("requests", IntegerType(), False)
])

# COMMAND ----------

(spark.read                   
  .option('header', 'true')   
  .option('sep', "\t")        
  .schema(csvSchema)          
  .csv(csvFile)               
  .printSchema()
)

# COMMAND ----------

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', "\t")
  .schema(csvSchema)
  .csv(csvFile)
)
print("Partitions: " + str(csvDF.rdd.getNumPartitions()) )
printRecordsPerPartition(csvDF)
print("-"*80)

# COMMAND ----------

