package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType}

object CustomerAmountSpent {
  case class OrderCSV(customerId: IntegerType ,noOfOrders:IntegerType,amountSpent:FloatType)
  def main(args: Array[String]) {

      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkContext using every core of the local machine
val spark=SparkSession
  .builder()
  .appName("CustomerSpend")
  .master("local[*]")
  .getOrCreate()

    import spark.implicits._

    val dataFromFile=spark.read
        .options(Map("inferSchema" -> "true","header"->"false"))
        .csv("data/customer-orders.csv")
      .toDF("cid","noOfOrders","amount")


    //val cleanedDataFrame=dataFromFile.withColumnRenamed("_c0","cid").withColumnRenamed("_c1","noOfOrders").withColumnRenamed("_c2","amount")
    val mostSpent=dataFromFile.groupBy("cid").sum("amount")
    val finalResult=mostSpent.withColumn("totalSpend",round($"sum(amount)",2)).select("cid","totalSpend").sort($"totalSpend"desc)
    val results=finalResult.collect()
          for(result<-results){
            print("CID:"+result(0))
            println("AmtSpent: "+result(1))
          }
    println(finalResult.explain())
  }

}
