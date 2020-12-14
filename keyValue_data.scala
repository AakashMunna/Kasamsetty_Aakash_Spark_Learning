// Databricks notebook source
val data = List("Aakash","Kasamsetty","spark","class","regex","Aakash")

// COMMAND ----------

val datardd = sc.parallelize(data)

// COMMAND ----------

datardd.countByValue()

// COMMAND ----------

val data2 = List(1,2,3,4,5)
val data2rdd = sc.parallelize(data2)

// COMMAND ----------

val productrdd = data2rdd.reduce((x,y)=> x*y)
productrdd

// COMMAND ----------


