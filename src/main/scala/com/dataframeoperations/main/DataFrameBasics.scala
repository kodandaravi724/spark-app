package com.dataframeoperations.main


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql

import org.apache.spark.sql.types._

object DataFrameBasics extends App {


  //Initialize Spark Session with application name DataFrame Basic ops
  //Set the master to local[*] to run locally using all available cores in machine

  val spark = SparkSession.builder()
    .appName("DataFrame Basic ops")
    .master("local[*]")
    .getOrCreate()

  //load the movie data into spark dataframe
  val movieDF: sql.DataFrame = spark.read.option("inferSchema", "true")
    .format("json").load("src/resources/data/movies_dupe.json")


  //prints schema of dataframe in console
  movieDF.printSchema()

  //get the count
  val movieRecordsCount = movieDF.count()

  print(s"Movie Record Count:- $movieRecordsCount")

  val movieSchema = StructType(Array(
    StructField("Creative_Type", StringType, true),
    StructField("Director", StringType, true),
    StructField("IMDB_Rating", DoubleType, true),
    StructField("IMDB_Votes", LongType, true)
  ))


  //dataframe is read with schema enforced. Prefer this method in prod as spark might infer schema incorrectly with option inferSchema = true
  val movieDf = spark.read.format("json").schema(movieSchema)
    .load("src/resources/data/movies_dupe.json")

//creating a dataframe from Seq

  val sampleData = Seq(("test1", "test1@gmail.com", "Tampa", 7654),
    ("test2", "test2@gmail.com", "Orlando", 543355),
    ("test3", "test3@gmail.com", "Tampa", 534245),
    ("test4", "test4@gmail.com", "Miami", 64534),
    ("test5", "test5@gmail.com", "Tampa", 4656),
    ("test6", "test6@gmail.com", "Detroit", 312321313),
    ("test7", "test7@gmail.com", "Columbus", 4324234),
    ("test8", "test8@gmail.com", "Tampa", 6754),
    ("test9", "test8@gmail.com", "Tampa", 3245),
    ("test10", "test10@gmail.com", "Tampa", 8675),
    ("test11", "test11@gmail.com", "Jacksonville", 8765),
    ("test12", "test12@gmail.com", "Tampa", 312321313),
    ("test13", "test13@gmail.com", "Tampa", 34256),
    ("test14", "test14@gmail.com", "Tampa", 4567))


  val dataFrame = spark.createDataFrame(sampleData)

  dataFrame.show(3)

  //Seq can be converted to datafrae using implicits

  import spark.implicits._

  val dataFrame2 = sampleData.toDF("name", "email", "city", "salary")

  dataFrame2.show(5)

  //Stop Spark Session
  // Failing to stop Spark session may lead to resource leakage, session limits, memory issues, and increased costs.
  spark.stop()

}
