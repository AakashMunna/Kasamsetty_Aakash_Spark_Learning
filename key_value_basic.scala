// Databricks notebook source
val listdata = List("tushar 2020","nancy 1998","abhinav 1997","karthik 2100")

// COMMAND ----------

val datardd = sc.parallelize(listdata)

// COMMAND ----------

val rddnew = datardd.map( x => (x.split(" ")(0), x.split(" ")(1).toInt))
rddnew.take(3)

// COMMAND ----------

rddnew.mapValues( x => x+10 ).take(2)

// COMMAND ----------


