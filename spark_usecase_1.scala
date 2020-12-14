// Databricks notebook source
// file of ratings.csv is in /FileStore/tables/ratings.csv

//creating an rdd from external dataset

val data = sc.textFile("/FileStore/tables/ratings.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

val ratingsdata = data.map(x => x.split(",")(2))

ratingsdata.collect()

// COMMAND ----------

ratingsdata.count()

// COMMAND ----------

ratingsdata.countByValue()

// COMMAND ----------

val airportsRDD = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

airportsRDD.collect()

// COMMAND ----------

// DBTITLE 1,First approach
val usAirport =  airportsRDD.filter( line => line.split(",")(3) == "\"United States\"")

usAirport.collect()

// COMMAND ----------

//defining a function

def splitInput(line:String) = {
  val dataSplit = line.split(",")
  val airportid = dataSplit(1)
  val cityname = dataSplit(2)
  
  (airportid,cityname)
}

// COMMAND ----------

usAirport.map(splitInput).take(2)

// COMMAND ----------

// DBTITLE 1,Second approach
usAirport.map( line => {
  val splitdata = line.split(",")
  splitdata(1)+" "+splitdata(2)
}).take(2)

// COMMAND ----------


