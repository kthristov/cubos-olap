package utad.producer

import java.util.Properties

import org.apache.spark.sql.{Encoders, SparkSession}

/**
  * Simple class for launching the DataProducer.
  */
object ProducerLauncher {

	def main(args: Array[String]): Unit = {

		// TODO read from conf file all parameters

		val topic = "test"
		val in = "in/*.csv"

		implicit val spark: SparkSession = SparkSession.builder()
			.master("local")
			.getOrCreate()

		val producer = new SparkProducer()
		producer.fillTopic(in, topic)

	}
}
