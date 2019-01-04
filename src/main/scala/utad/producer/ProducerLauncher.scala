package utad.producer

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Simple class for launching the DataProducer.
  */
object ProducerLauncher {

	def main(args: Array[String]): Unit = {

		// Reading conf
		val fileContents = Source.fromFile("conf/app.conf").getLines.mkString("\n")
		val config: Config = ConfigFactory.parseString(fileContents)

		val kafkaHost: String = config.getString("kafka.host")
		val kafkaPort: String = config.getString("kafka.port")
		val topic: String = config.getString("kafka.olapTopic")
		val in: String = config.getString("hdfs.in")
		val sparkMaster: String = config.getString("spark.master")

		val kafka = kafkaHost + ":" + kafkaPort

		// TODO spark session creation by extending trait
		// Removing spark info log
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		// Generating  SparkSession
		implicit val spark: SparkSession = SparkSession.builder()
			.master(sparkMaster)
			.getOrCreate()

		val producer = new SparkProducer(kafka)
		producer.fillTopic(in, topic)

	}
}
