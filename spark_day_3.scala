// Databricks notebook source
// DBTITLE 1,Intersection operation
// File uploaded to /FileStore/tables/nasa_july-1.tsv
// File uploaded to /FileStore/tables/nasa_august-1.tsv

val julyrdd = sc.textFile("/FileStore/tables/nasa_july-1.tsv")
val augustrdd = sc.textFile("/FileStore/tables/nasa_august-1.tsv")

// COMMAND ----------

val julyRdd = julyrdd.map(line => line.split("\t")(0))
val augustRdd = augustrdd.map(line => line.split("\t")(0))
julyRdd.collect()
augustRdd.collect()

// COMMAND ----------

val interRdd = julyRdd.intersection(augustRdd)

interRdd.count()

// COMMAND ----------


