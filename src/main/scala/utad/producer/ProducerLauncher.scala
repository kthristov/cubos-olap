package utad.producer

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Encoders, SparkSession}

import scala.io.Source

/**
  * Simple class for launching the DataProducer.
  */
object ProducerLauncher {

	def main(args: Array[String]): Unit = {

		// Reading conf
		val fileContents = Source.fromFile("conf/app.conf").getLines.mkString("\n")
		val config: Config = ConfigFactory.parseString(fileContents)

		val topic = config.getString("kafka.olapTopic")
		val in = config.getString("hdfs.in")

		implicit val spark: SparkSession = SparkSession.builder()
			.master("local")
			.getOrCreate()

		val producer = new SparkProducer()
		producer.fillTopic(in, topic)

	}
}
