package utad

import java.sql.Timestamp

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import utad.consumer.SparkConsumer
import utad.utils.TimeUtils._

import scala.io.Source


/**
  * @author ${user.name}
  */
object App {

	def main(args: Array[String]) {

		// Removing spark info log
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		// Reading conf
		val fileContents = Source.fromFile("conf/app.conf").getLines.mkString("\n")
		val config: Config = ConfigFactory.parseString(fileContents)

		val kafkaTopic = config.getString("kafka.olapTopic")

		val cassandraHost = config.getString("cassandra.host")
		val keyspace = config.getString("cassandra.olapKeyspace")
		val table = config.getString("cassandra.olapTable")

		val hdfsOlapPath = config.getString("hdfs.tripsPath")

		val sparkTriggerInterval = config.getString("spark.triggerInterval")

		// Generating  SparkSession
		implicit val spark: SparkSession = SparkSession.builder()
			.config("spark.cassandra.connection.host", cassandraHost)
			.master("local")
			.getOrCreate()

		// spark udfs
		val roundToHour = udf[Int, Timestamp](roundTimestampToHour)
		val uuid = udf(() => java.util.UUID.randomUUID().toString)

		// Generating input stream
		val stream = new SparkConsumer().readTopic(kafkaTopic)

		// Filtering useless rows.
		val query = stream.filter(!_.isNullAt(0)) // Remove rows with null id.
		// TODO some more data filtering (outliers..)

		query
			.writeStream
			.trigger(Trigger.ProcessingTime(sparkTriggerInterval))
			.foreachBatch { (batchDF: DataFrame, batchId: Long) =>

				batchDF.cache()

				// Debug
				println(batchDF.count())

				// Writing to HDFS
				batchDF.write
					.mode("append")
					.format("parquet")
					.save(hdfsOlapPath)

				// Writing aggregates to Cassandra
				batchDF.select("pu_location_id", "pickup_dt", "dropoff_dt", "distance", "total_amount")
					.withColumn("pickup_dt", roundToHour(col("pickup_dt")))
					.groupBy("pickup_dt", "pu_location_id").agg(
						sum("distance").alias("distance"),
						sum("total_amount").alias("total_amount"))
					// sets names acording to C* columns
					.withColumnRenamed("pu_location_id", "pickup_location")
					.withColumnRenamed("pickup_dt", "pickup_hour")
					.withColumn("id", uuid())  //avoid repeated columns (C* omits them)
					.write
		            .format("org.apache.spark.sql.cassandra")
	                .options(Map("keyspace"->keyspace,
		                         "table"->table))
		            .mode(SaveMode.Append)
	                .save()

				batchDF.unpersist()

			}
			.start()
			.awaitTermination()
	}

}
