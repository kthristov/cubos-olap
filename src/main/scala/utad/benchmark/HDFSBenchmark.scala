package utad.benchmark

import java.sql.Timestamp

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import utad.utils.TimeUtils.roundTimestampToHour

import scala.io.Source


/**
  * Executes a simple group by query (the same defined in App.scala) and averages the time
  * taken for it by 10 queries.
  */
object HDFSBenchmark {

	def main(args: Array[String]) {

		// Reading conf
		val fileContents: String = Source.fromFile("conf/app.conf").getLines.mkString("\n")
		val config: Config = ConfigFactory.parseString(fileContents)

		val hdfsOlapPath: String = config.getString("hdfs.tripsPath")

		val sparkMaster: String = config.getString("spark.master")

		// TODO spark session creation by extending trait
		// Removing spark info log
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		// Generating  SparkSession
		val spark: SparkSession = SparkSession.builder()
			.master(sparkMaster)
			.getOrCreate()

		for (_ <- 1 to 10) {
			println(getQueryTime(spark, hdfsOlapPath))
		}
	}

	def getQueryTime(spark: SparkSession, path: String): Long = {

		val startInstant = System.currentTimeMillis() // registers start of reading

		val roundToHour = udf[Int, Timestamp](roundTimestampToHour)

		val df = spark.read.parquet(path)

		df.select("pu_location_id", "pickup_dt", "dropoff_dt", "distance", "total_amount")
			.withColumn("pickup_dt", roundToHour(col("pickup_dt")))
			.groupBy("pickup_dt", "pu_location_id").agg(
			sum("distance").alias("distance"),
			sum("total_amount").alias("total_amount"))
			.withColumn("distance", round(col("distance"), 3))
			.withColumn("total_amount", round(col("total_amount"), 2))
			.filter(_.getDouble(2) > 0) // avoids infinites
			.withColumn("km_price", round(col("distance") / col("total_amount"), 3))

		val endInstant = System.currentTimeMillis()

		endInstant - startInstant
	}
}
