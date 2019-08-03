package com.acme

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._

object App {

  def process() = {

    val sparkConfig = new SparkConf().setAppName("MetricsTest").setMaster("local[*]")

    val spark = SparkSession.builder.config(sparkConfig)
      .getOrCreate()

    import spark.implicits._

    val uuid = UUID.randomUUID().toString

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .option("numPartitions", 10)
      .option("rampUpTime", 3 + "s")
      .load()
      .select(
        $"timestamp",
        $"value",
        lit(uuid) as 'user_id,
        lit(uuid) as 'advert_id,
        lit(s"$uuid - acmeuser") as 'name)

    // DO YOUR STUFF HERE

    val result = df.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .option("truncate", "false")
      .start()


    result.awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    process()
  }

}