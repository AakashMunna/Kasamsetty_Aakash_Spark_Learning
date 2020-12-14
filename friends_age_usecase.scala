// Databricks notebook source
val data = sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

data.take(2)

// COMMAND ----------

val headerremover = data.filter( line => !line.contains("friends"))
headerremover.take(10)

// COMMAND ----------

// DBTITLE 1,Finding average number of friends for each age group
val friendsrdd = headerremover.map(x => (x.split(",")(2).toInt, (1,x.split(",")(3).toInt)))
friendsrdd.take(10)

// COMMAND ----------

val reducedrdd = friendsrdd.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
reducedrdd.collect()

// COMMAND ----------

val finalrdd = reducedrdd.mapValues( data => data._2 / data._1)
finalrdd.collect()

// COMMAND ----------

// DBTITLE 1,finding max number of friends for each age group
val friends2rdd = headerremover.map( x => (x.split(",")(2).toInt, x.split(",")(3).toInt))

friends2rdd.take(10)

// COMMAND ----------

val maxrdd = friends2rdd.reduceByKey(math.max(_,_))

maxrdd.collect()

// COMMAND ----------


