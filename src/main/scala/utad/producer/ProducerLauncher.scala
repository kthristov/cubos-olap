package utad.producer

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.spark.sql.SparkSession

//Simple class for launching the DataProducer
object ProducerLauncher {

	def main(args: Array[String]): Unit = {

		// TODO read conf file

		val topic = "yellow_cabs"
		val filePath = args(0)

		val spark = SparkSession.builder()
								.master("local")
								.getOrCreate()

		val dataProducer = new DataProducer()
		dataProducer.fillTopic(filePath, topic)

	}
}