// Databricks notebook source
// DBTITLE 1,Set operation union
// File uploaded to /FileStore/tables/nasa_august.tsv
// File uploaded to /FileStore/tables/nasa_july.tsv

val julyrdd = sc.textFile("/FileStore/tables/nasa_july.tsv")
val augustrdd = sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

val unionrdd = julyrdd.union(augustrdd)
unionrdd.take(2)

// COMMAND ----------

// DBTITLE 1,Set first row as header
val header = unionrdd.first

// COMMAND ----------

// DBTITLE 1,to display data without header
unionrdd.filter(line => line != header).take(3)

// COMMAND ----------

// DBTITLE 1,alternate method to remove header using function
def headerRemover(line: String): Boolean = !(line.startsWith("host"))

// COMMAND ----------

val finalrdd = unionrdd.filter( x => headerRemover(x))
finalrdd.take(2)

// COMMAND ----------

// DBTITLE 1,Sampling the data
finalrdd.sample(withReplacement = true, fraction = 0.20).collect()

// COMMAND ----------


