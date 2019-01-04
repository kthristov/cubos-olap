package utad.producer

import org.apache.spark.sql.{Encoders, SparkSession}
import utad.data.Trip


/**
  * Simple class when executed feeds a kafka topic with data read from given csv file.
  */
class SparkProducer(kafkaHost: String)(implicit spark: SparkSession) {

	val kafka: String = kafkaHost

	/**
	  * Given path reads files and writes all data to given kafka topic
	  *
	  * @param dir
	  * @param topic
	  * @param spark
	  */
	def fillTopic(dir: String, topic: String): Unit = {

		import spark.implicits._

		val schema = Encoders.product[Trip].schema // TODO parametrize csv row schema

		val trips = spark.readStream
			.schema(schema)
			.csv(dir)
			.as[Trip]

		val stream = trips
			.selectExpr("to_json(struct(*)) AS value")
			.writeStream
			.format("kafka")
			.option("topic", topic)
			.option("kafka.bootstrap.servers", kafka)
			.option("checkpointLocation", "res/checkpoint")
			.start()

		stream.awaitTermination()
	}
}



