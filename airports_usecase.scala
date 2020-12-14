// Databricks notebook source
val airport = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

airport.collect()

// COMMAND ----------

val country = airport.filter(x=>x.split(",")(3) == "\"United States\"").collect().take(3)

// COMMAND ----------

val task = airport.map(x=>(x.split(",")(9).toFloat, x.split(",")(11)))

task.take(20)

// COMMAND ----------

val fin=task.filter(x=> x._1 % 2.0 == 0)

fin.take(20)

// COMMAND ----------

fin.countByValue().take(20)

// COMMAND ----------

val t1=airport.map(x=>(x.split(",")(3), x.split(",")(7).toDouble))
t1.take(10)

// COMMAND ----------

t1.filter(x=> x._1 == "\"Iceland\"" && x._2 > -16.0 ).collect()

// COMMAND ----------


