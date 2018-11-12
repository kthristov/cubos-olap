package utad.producer

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.OutputMode


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


case class Trip(
	               vendor_id: Long,
	               pickup_dt: String,
	               dropoff_dt: String,
	               passengers: Integer,
	               distance: Float,
	               ratecode_id: Integer,
	               store_fwd_flag: String,
	               pu_location_id: Integer,
	               do_location_id: Integer,
	               payment_type: Integer,
	               fare_amount: Float,
	               extra: Float,
	               mta_max: Float,
	               tip_amount: Float,
	               tolls_amount: Float,
	               improvement_surcharge: Float,
	               total_amount: Float
                )

