// Databricks notebook source
// /FileStore/tables/numberData.csv

val data = sc.textFile("/FileStore/tables/numberData.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

def headerRemover(line: String): Boolean = !(line.startsWith("Number"))

// COMMAND ----------

val finalrdd = data.filter( x => headerRemover(x))
finalrdd.take(2)

// COMMAND ----------

// DBTITLE 1,Defining a function for extracting prime numbers
// def isprime(n: Int): Boolean = ! ((2 until n-1) exists (n % _ == 0)) ---> it gives "1" as a prime number

def isPrime2(i :Int) : Boolean = {
  if (i <= 1)
       false
     else if (i == 2)
       true
     else
       !(2 to (i-1)).exists(x => i % x == 0)
   }

// COMMAND ----------

val numberRdd = finalrdd.map(x => x.toInt)
numberRdd.take(10)

// COMMAND ----------

numberRdd.filter(i => (isPrime2(i))).sum()

// COMMAND ----------


