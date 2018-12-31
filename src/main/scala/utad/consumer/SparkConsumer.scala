package utad.consumer

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import utad.data.Trip

class SparkConsumer {

	/**
	  * Method which subscribes to the given topic and returns Dataset to be processed.
	  * Return stream must be started.
	  * @param topic Topic to read from.
	  * @param spark SparkSession for reading from Kafka.
	  * @return Dataset to be processed.
	  */
	def readTopic(topic: String)(implicit spark: SparkSession): DataFrame = {

		import spark.implicits._

		val schema = Encoders.product[Trip].schema

		val df = spark
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", "localhost:9092")
			.option("startingOffsets", "earliest")
			.option("subscribe", topic)
			.load()

		val stream = df
			.selectExpr("CAST(value AS STRING)")
			.as[String]
			.select(from_json($"value", schema).as("data"))
			.select("data.*")

		stream
	}
}
