# Databricks notebook source
# MAGIC %md
# MAGIC #Dataframe 

# COMMAND ----------

##To Load myfile from FileStore in Databricks location
#Split out the delimeters and do some cleaning
myrdd = sc.textFile('/FileStore/tables/clinicaltrial_2023.csv')
myrdd2 = myrdd.map(lambda line: line.replace(",", "").replace('"',''))
myrdd3 = myrdd2.map(lambda s: s.split("\t")) 

# COMMAND ----------

#import the necessary libraries
from pyspark.sql.functions import *
from pyspark.sql import *

# COMMAND ----------

# Collect length of the first row
first_row_length = len(myrdd3.first())

# Filter out rows with incomplete columns
myrdd_complete = myrdd3.filter(lambda row: len(row) == first_row_length)


# COMMAND ----------

#Convert my Rdd to Dataframe
Df = myrdd_complete.toDF()

# COMMAND ----------

Df.display()

# COMMAND ----------

#Naming my Schema
renaming_mapping = {
    "_1": "Id","_2": "Study Title","_3": "Acronym","_4": "Status","_5": "Conditions","_6": "Interventions","_7": "Sponsor","_8": "Collaborators",
    "_9": "Enrollment","_10": "Funder Type","_11": "Type","_12": "Study Design","_13": "Start","_14": "Completion"
}

# Renaming columns using withColumnsRenamed
df_renamed = Df
for old_name, new_name in renaming_mapping.items():
    df_renamed = df_renamed.withColumnRenamed(old_name, new_name)


#Removing my duplicated header in my first Column
condit = df_renamed['Study Title'] != 'Study Title'
Df2 = df_renamed.filter(condit)
Df2.show(10)

# COMMAND ----------

Df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #1. The number of studies in the dataset

# COMMAND ----------

# Count the number of distinct study Type
num_studies = Df2.select("Study Title").distinct().count()

print("Number of studies in the dataset:", num_studies)

# COMMAND ----------

# MAGIC %md
# MAGIC #2. list all the types

# COMMAND ----------

# Using DataFrame to find types of studies with frequencies

typesA = Df2.groupBy("Type").count().orderBy(col("count").desc())
#Remove the ones without Type name
typesA_filtered = typesA.filter(typesA["Type"] != "")
typesA_filtered.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #3. The top 5 conditions (from Conditions) with their frequencies.

# COMMAND ----------

#Mapping out the required index
cnd1 = myrdd2.map(lambda s: s.split("\t")[4])

# COMMAND ----------

#flatMapping
cnd2 = cnd1.flatMap(lambda row: row.split("|"))

# Create Row objects from RDD elements
cnd3 = cnd2.map(lambda x: Row(value=x))

# Create DataFrame from Row objects
cnd4 = spark.createDataFrame(cnd3, ['Conditions'])

# Using DataFrame to find top 5 conditions with frequencies
conditions = cnd4.groupBy("Conditions").count().orderBy(col("count").desc()).limit(5)
conditions.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #4. Find the 10 most common sponsors that are not pharmaceutical companies

# COMMAND ----------

#Cleaning the pharma file
phrdd = sc.textFile('/FileStore/tables/pharma.csv')
phrdd1 = phrdd.map(lambda line: line.replace('"', ''))
phrdd2 = phrdd1.map(lambda s:(s.split(",")[1] ))

# COMMAND ----------

# Create Row objects from RDD elements
row_rdd = phrdd2.map(lambda x: Row(value=x))
# Create DataFrame from Row objects
df = spark.createDataFrame(row_rdd, ['Parent_Company'])
# Filter out rows where the value in the "Parent_Company" column is not equal to "Parent_Company"
df_filtered = df.filter(df["Parent_Company"] != "Parent_Company")
df1 = df_filtered.select("Parent_Company").distinct()


# COMMAND ----------

#Remove those present in Pharm table that are in clinical trial table (sponsor column)
most_common_sponsors = Df2.join(df1, Df2["Sponsor"] == df1["Parent_Company"], "left_anti")
sponsor_counts = most_common_sponsors.groupBy("Sponsor").count().orderBy(col("count").desc())
sponsor_counts.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #5. Plot number of completed studies for each month in 2023

# COMMAND ----------

#Filter out the required column for this question
date = Df2.select('Completion', 'Status')
#Filter out the completed status
date1 = date.filter(date['Status'].like('%COMPLETED%'))
#Filter out the those in 2023
date2 = date1.filter(date1['Completion'].like('2023%'))
#slice through to filter the month and year
date3 = date2.withColumn('Year', split('Completion','-')[0]).withColumn('Month', split('Completion','-')[1])

# COMMAND ----------

monthly_counts = date3.groupBy('Month').count().orderBy('Month', ascending=True)
monthly_counts.display()
