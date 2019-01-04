package utad.benchmark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, round, sum}

import scala.io.Source

object CassandraBenchmark {

	def main(args: Array[String]) {

		// Removing spark info log
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		// Reading conf
		val fileContents: String = Source.fromFile("conf/app.conf").getLines.mkString("\n")
		val config: Config = ConfigFactory.parseString(fileContents)

		val cassandraHost: String = config.getString("cassandra.host")
		val keyspace: String = config.getString("cassandra.olapKeyspace")
		val table: String = config.getString("cassandra.olapTable")

		val sparkMaster: String = config.getString("spark.master")

		// Generating  SparkSession // TODO spark session creation by extending trait
		val spark: SparkSession = SparkSession.builder()
			.config("spark.cassandra.connection.host", cassandraHost)
			.master(sparkMaster)
			.getOrCreate()

		for (_ <- 1 to 10) {
			println(getQueryTime(spark, keyspace, table))
		}
	}

	def getQueryTime(spark: SparkSession, keyspace: String, table: String): Long = {

		val startInstant = System.currentTimeMillis() // registers start of reading

		val df = spark
			.read
			.format("org.apache.spark.sql.cassandra")
			.options(Map("keyspace" -> keyspace,
						 "table" -> table))
			.load()

		df.select("pickup_location", "pickup_hour", "distance", "total_amount")
			.groupBy("pickup_hour", "pickup_location").agg(
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
