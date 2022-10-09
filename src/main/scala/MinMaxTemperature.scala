package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object MinMaxTemperature {
  case class Temperature(sID: String, date: Int, measure_type:String,temperature: Float)
  def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark= SparkSession
      .builder()
      .appName("MinMaxTemperature")
      .master("local[*]")
      .getOrCreate()

    val temperatureSchema= new StructType()
      .add("SID",StringType,nullable=true)
      .add("date",IntegerType,nullable=true)
      .add("measure_type",StringType,nullable=true)
      .add("temperature",FloatType,nullable=true)

    //Instead of getting from header we use struct type. Before we used a header and used infer scheme and then made it into a dataset by using as the case class
    import spark.implicits._
    val ds=spark.read
      .schema(temperatureSchema)
      .csv("data/1800.csv")
      .as[Temperature]

    val minTemps=ds.filter($"measure_type"==="TMAX")
    //=== and =!= used in the filter () for this library
    val stationTemps=minTemps.select("SID","temperature")
    val minTempsByStation=stationTemps.groupBy("SID").max("temperature")
    val sortedminTempsByStation=minTempsByStation
      .withColumn("temperature",round($"max(temperature)"*0.1f,2))
      .select("SID","temperature").sort($"temperature"desc)


    val results= sortedminTempsByStation.collect()

    for(result <- results)
      {
        val station= result(0)
        val temp= result(1).asInstanceOf[Float]
        val formattedTemp=f"$temp%.2f F"
        println(s"Station: $station Max Temperature: $formattedTemp")
      }
  }
}
