// Databricks notebook source
// /FileStore/tables/Property_data.csv

val property = sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

property.take(2)

// COMMAND ----------

val headerremover = property.filter( line => !line.contains("Price"))
headerremover.take(10)

// COMMAND ----------

val roomrdd = headerremover.map(x => (x.split(",")(3).toInt, (1,x.split(",")(2).toDouble)))
roomrdd.collect()

// COMMAND ----------

val reducedrdd = roomrdd.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
reducedrdd.collect()

// COMMAND ----------

val finalrdd = reducedrdd.mapValues( data => data._2 / data._1)
finalrdd.collect()

// COMMAND ----------

for((bedroom, avg) <- finalrdd.collect())println(bedroom + " : " + avg)

// COMMAND ----------

finalrdd.saveAsTextFile("PropertyFinal.csv")

// COMMAND ----------


