# Databricks notebook source
# MAGIC %md
# MAGIC #RDD TASK

# COMMAND ----------

#list all the files in the databricks tables
dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

#To copy a file from a source location in DBFS to a destination location in the local file system /tmp/
# assigned fileroot to my file.
fileroot = "clinicaltrial_2023"
dbutils.fs.cp("/FileStore/tables/" + fileroot + ".zip", "file:/tmp/")

# COMMAND ----------

#import the OS environment
import os
os.environ['fileroot'] = fileroot

# COMMAND ----------

# MAGIC %sh
# MAGIC #Linux comand used to unzip my files
# MAGIC unzip -o -d /tmp /tmp/$fileroot.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/
# MAGIC # list my files in my file location to check to be sure 

# COMMAND ----------

#create a directory and move a file from a source location in the local file system (/tmp/) to a destination location in DBFS.
dbutils.fs.mv("file:/tmp/" + fileroot + ".csv", "/FileStore/tables/" + fileroot + ".csv" , True)

# COMMAND ----------

#As usuall ill list to check if my file is in place
dbutils.fs.ls("/FileStore/tables/" + fileroot + '.csv')

# COMMAND ----------

#Display the first few lines of the file
dbutils.fs.head("/FileStore/tables/" + fileroot + '.csv' )

# COMMAND ----------

#Converting it to a textfile using RDD method 
myrdd = sc.textFile("/FileStore/tables/" + fileroot + '.csv')
#Lets see how it looks, so we do take(2)
myrdd.take(2)

# COMMAND ----------

#Removing inrelevant characters in my data. 
# Spliting with the know delimiter i can see
myrdd1 = myrdd.map(lambda line: line.replace(",", "").replace('"',''))
myrdd2 = myrdd1.map(lambda s: s.split("\t")) 

# COMMAND ----------

# Filter out rows with incomplete columns
first_row_length = len(myrdd2.first())
myrdd_complete = myrdd2.filter(lambda row: len(row) == first_row_length)

# COMMAND ----------

#Lets see how it looks now. we do take(2)
myrdd_complete.take(5)

# COMMAND ----------

##Display the first few lines of the file
#Now we working on our second file. since our zip code is reuseable we just have to replace the fileroodt name with our new file in the zip function
dbutils.fs.head("/FileStore/tables/pharma.csv" )

# COMMAND ----------

##Converting it to a textfile using RDD method 
phrdd = sc.textFile('/FileStore/tables/pharma.csv')
phrdd.take(2)

# COMMAND ----------

##From the previous Rdd we noticed  unwanted things that made the file looks rough
#Now we clean the file so it looks easier to understand using the .replace command.  
phrdd1 = phrdd.map(lambda line: line.replace('"', ''))
phrdd1.take(2)

# COMMAND ----------

#i will just be working with the content in index 1, so ill map it out
phrdd2 = phrdd1.map(lambda s:(s.split(",")[1] ))

# COMMAND ----------

#As usual check to see the result. ill be doing take(5)
phrdd2.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #1. The number of studies in the dataset. 

# COMMAND ----------

#Mapping out my studies index 
myrdd5 = myrdd_complete.map(lambda s: (s[1], 1))
#Remove the Header so we dont count it also
myrdd3 = myrdd5.filter(lambda x: "Study Title" not in x)
myrdd3.take(5)

# COMMAND ----------

#To reduce by key so it counts all distinct studies only
myrdd6 = myrdd3.reduceByKey(lambda a,b: a + b)
#Now i count
myrdd6.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #2. You should list all the types

# COMMAND ----------

#Mapping out the index with Types in it
myrdd7 = myrdd_complete.map(lambda s: (s[10], 1))
#Lets remove the header
myrdd8 = myrdd7.filter(lambda x: "Type" not in x)
myrdd8.take(3)

# COMMAND ----------

#Use reduce by key to get the distinct all count together 
myrdd9 = myrdd8.reduceByKey(lambda a,b: a + b).filter(lambda s: s[0] != '')
#I want it from high to low,so i sort it by decending order
myrdd10 = myrdd9.sortBy(lambda s: s[1], ascending = False)
myrdd10.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #3. The top 5 conditions

# COMMAND ----------

#Mapping out conditions from our rdd
myrdd11 = myrdd1.map(lambda s: (s.split("\t")[4] ))
#I noticed that our data still needs some work and more cleaning is needed. Now we flatmap because we need our data just as we inputed. 
myrdd12 = myrdd11.flatMap(lambda row: row.split("|"))
myrdd12.take(3)

# COMMAND ----------

# Map each element to a key-value pair where the element is the key and the value is 1
myrdd13 = myrdd12.map(lambda x: (x, 1))

# Use reduceByKey to find the frequency of each element
myrdd14 = myrdd13.reduceByKey(lambda a, b: a + b)

# COMMAND ----------

#Arrange it with sortBy from Descending order
myrdd15 = myrdd14.sortBy(lambda s: s[1], ascending = False)
myrdd15.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #4. Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored.

# COMMAND ----------

# Remove duplicates on the Pharm table
phrdd3 = phrdd2.distinct()

# Filtter out my sponsor column
clrrd = myrdd_complete.map(lambda s: (s[6]))
clrrd1 = clrrd.filter(lambda x: "Sponsor" not in x)
#Remove those present in Pharm table that are in clinical trial table (sponsor column)
Ans = clrrd1.subtract(phrdd3)

# COMMAND ----------

# Use reduceByKey to find the frequency of each element
Ans1 = Ans.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
#Arrange it with sortBy from Descending order
Ans_frequencies = Ans1.sortBy(lambda x: x[1], ascending = False)

# Print the result
Ans_frequencies.take(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ##5. Plot number of completed studies for each month in 2023. You need to include your visualization as well as a table of all the values you have plotted for each month

# COMMAND ----------

# Filter out column in index 3 and 13
plrdd1 = myrdd_complete.map(lambda s: (s[3], s[13]))
# Filter out othe completed ones
plrdd2 = plrdd1.filter(lambda s: s[0] != 'Completion').filter(lambda s: s[0] == 'COMPLETED') or filter(lambda s:s[0] == 'Completed')

# COMMAND ----------

#Slicing it
plrdd3 = plrdd2.map(lambda s: (s[1][5:7], s[1][0:4]))
#filter out ones in 2023
plrdd4 = plrdd3.filter(lambda s: s[1] == '2023')
plrdd4.take(6)

# COMMAND ----------

# Use reduceByKey to find the frequency of each element
plrdd5 = plrdd4.map(lambda s: (s[0], 1)).reduceByKey(lambda a,b: a+b)
#Arrange it with sortBy from ascending order
plrdd6 = plrdd5.sortBy(lambda s: s[0], ascending = True).collect()
print("Table of Completed Studies in 2023 by Month:")
for month, count in plrdd6:
    print(f"{month}\t{count}")
    #table of all the values

# COMMAND ----------

import matplotlib.pyplot as plt

# Extracting months and counts for plotting
months = [month for month, _ in plrdd6]
counts = [count for _, count in plrdd6]

# Plotting

plt.figure(figsize=(10, 6))
plt.bar(months, counts, color='blue')
plt.xlabel('Month')
plt.ylabel('Number of Completed Studies')
plt.title('Completed Studies in 2023 by Month')
plt.xticks(range(1, 13))
plt.show()


# COMMAND ----------


# Extracting months and counts for plotting
months = [month for month, _ in plrdd6]
counts = [count for _, count in plrdd6]
plt.figure(figsize=(10, 6))
plt.plot(months, counts, marker='o', color='skyblue', linestyle='-')
plt.xlabel('Month')
plt.ylabel('Number of Completed Studies')
plt.title('Completed Studies in 2023 by Month')
plt.xticks(range(1, 13))
plt.grid(True)
plt.show()
