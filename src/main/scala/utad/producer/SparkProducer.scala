package utad.producer

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.OutputMode
import utad.data.Trip


/**
  * Simple class when executed feeds a kafka topic with data read from given csv file.
  */
class SparkProducer {

	/**
	  * Given path reads files and writes all data to given kafka topic
	  * @param dir
	  * @param topic
	  * @param spark
	  */
	def fillTopic(dir: String, topic: String)(implicit spark: SparkSession): Unit = {

		import spark.implicits._

		val schema = Encoders.product[Trip].schema

		val trips = spark.readStream
			.schema(schema)
			.csv(dir)
			.as[Trip]

		val stream = trips
			.selectExpr("to_json(struct(*)) AS value")
			.writeStream
			.format("kafka")
			.option("topic", topic)
			.option("kafka.bootstrap.servers", "localhost:9092") // TODO coger de configuración
			.option("checkpointLocation", "res/checkpoint")
			.start()

		stream.awaitTermination()
	}
}



