#Basic DataBricks magic commands and DBFS Practice

# Databricks notebook source
print("I'm running Python")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC println("Scala!")

# COMMAND ----------

# MAGIC %run "/Users/piyush.u.kumar@avanade.com/01-Introduction-to-Azure-Databricks/Includes/Classroom-Setup"

# COMMAND ----------

print(username)

# COMMAND ----------

print(userhome)

# COMMAND ----------

mounts = dbutils.fs.mounts()

for mount in mounts : 
  print(mount.mountPoint + " >> " + mount.source)
  
print("-"*80)

# COMMAND ----------

files = dbutils.fs.ls("/mnt/training/")

for fileInfo in files:
  print(fileInfo.path)

print("-"*80)

# COMMAND ----------

display(files)

# COMMAND ----------

