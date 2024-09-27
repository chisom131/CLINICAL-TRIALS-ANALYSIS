-- Databricks notebook source
-- MAGIC %python
-- MAGIC myrdd = sc.textFile('/FileStore/tables/clinicaltrial_2023.csv')
-- MAGIC myrdd2 = myrdd.map(lambda line: line.replace(",", "").replace('"',''))
-- MAGIC myrdd3 = myrdd2.map(lambda s: s.split("\t")) 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Collect length of the first row
-- MAGIC first_row_length = len(myrdd3.first())
-- MAGIC
-- MAGIC # Filter out rows with incomplete columns
-- MAGIC myrdd_complete = myrdd3.filter(lambda row: len(row) == first_row_length)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Df = myrdd_complete.toDF()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Df.show(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC renaming_mapping = {
-- MAGIC     "_1": "Id","_2": "Study Title","_3": "Acronym","_4": "Status","_5": "Conditions","_6": "Interventions","_7": "Sponsor","_8": "Collaborators",
-- MAGIC     "_9": "Enrollment","_10": "Funder Type","_11": "Type","_12": "Study Design","_13": "Start","_14": "Completion"
-- MAGIC }
-- MAGIC
-- MAGIC # Renaming columns using withColumnsRenamed
-- MAGIC df_renamed = Df
-- MAGIC for old_name, new_name in renaming_mapping.items():
-- MAGIC     df_renamed = df_renamed.withColumnRenamed(old_name, new_name)
-- MAGIC
-- MAGIC # Displaying the renamed DataFrame
-- MAGIC df_renamed.show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_renamed.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Remove headers from column 1
-- MAGIC
-- MAGIC Dff2 = df_renamed['Study Title'] != 'Study Title'
-- MAGIC Df2 = df_renamed.filter(Dff2)
-- MAGIC Df2.show(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Df2.createOrReplaceTempView ("Clinical")

-- COMMAND ----------

select *from Clinical limit 10

-- COMMAND ----------

Show Tables

-- COMMAND ----------

select * from clinical

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #1. The number of studies in the dataset.

-- COMMAND ----------

SELECT COUNT(DISTINCT `Study Title`) AS num_studies
FROM clinical


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #2. You should list all the types

-- COMMAND ----------

SELECT Type, COUNT(*) AS Frequency
FROM clinical
WHERE Type <> ''
GROUP BY Type
ORDER BY Frequency DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #3. The top 5 conditions (from Conditions) with their frequencies

-- COMMAND ----------

-- MAGIC %python
-- MAGIC myrdd4 = myrdd2.map(lambda s: s.split("\t")[4])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC myrdd4.take(3)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC myrdd5 = myrdd4.flatMap(lambda row: row.split("|"))
-- MAGIC
-- MAGIC
-- MAGIC # Create Row objects from RDD elements
-- MAGIC myrdd6 = myrdd5.map(lambda x: Row(value=x))
-- MAGIC
-- MAGIC # Create DataFrame from Row objects
-- MAGIC myrdd7 = spark.createDataFrame(myrdd6, ['Conditions'])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC myrdd7.createOrReplaceTempView ("cond")

-- COMMAND ----------

SELECT Conditions, COUNT(*) AS Frequency
FROM cond
GROUP BY Conditions
ORDER BY Frequency DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #4. Find the 10 most common sponsors that are not pharmaceutical companies

-- COMMAND ----------

-- MAGIC %python
-- MAGIC phrdd = sc.textFile('/FileStore/tables/pharma.csv')
-- MAGIC phrdd1 = phrdd.map(lambda line: line.replace('"', ''))
-- MAGIC phrdd2 = phrdd1.map(lambda s:(s.split(",")[1] ))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create Row objects from RDD elements
-- MAGIC row_rdd = phrdd2.map(lambda x: Row(value=x))
-- MAGIC
-- MAGIC # Create DataFrame from Row objects
-- MAGIC df = spark.createDataFrame(row_rdd, ['Parent_Company'])
-- MAGIC
-- MAGIC # Show the filtered DataFrame
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView ("Pham")

-- COMMAND ----------

Show Tables

-- COMMAND ----------

select *from Pham limit 10

-- COMMAND ----------

 SELECT Sponsor, COUNT(*) AS Tot
    FROM clinical
    LEFT ANTI JOIN Pham
    ON clinical.Sponsor = Pham.Parent_Company
    GROUP BY clinical.Sponsor
    ORDER BY Tot DESC
    LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #5 number of completed studies for each month in 2023

-- COMMAND ----------

 SELECT 
        SUBSTRING(Completion, 6, 2) AS Month,
        COUNT(*) AS Completed_Studies
    FROM clinical
    WHERE Status = 'COMPLETED' AND SUBSTRING(Completion, 1, 4) = '2023'
    GROUP BY SUBSTRING(Completion, 6, 2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Additional Analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## list all sponsor and the total enrollment they did

-- COMMAND ----------

SELECT sponsor, SUM(enrollment) AS total_enrollment
FROM clinical
GROUP BY sponsor;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Number of completed studies for each month in 2016

-- COMMAND ----------

 SELECT 
        SUBSTRING(Completion, 6, 2) AS Month,
        COUNT(*) AS Completed_Studies
    FROM clinical
    WHERE Status = 'COMPLETED' AND SUBSTRING(Completion, 1, 4) = '2016'
    GROUP BY SUBSTRING(Completion, 6, 2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Find the Sponsors without enrollment
-- MAGIC  

-- COMMAND ----------

SELECT Sponsor, Enrollment
FROM clinical
WHERE Enrollment = '';
