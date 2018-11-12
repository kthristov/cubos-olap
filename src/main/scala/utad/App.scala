package utad

import org.apache.spark.sql.SparkSession
import utad.consumer.SparkConsumer

/**
  * @author ${user.name}
  */
object App {

	def main(args: Array[String]) {

		// TODO read conf from external conf file.

		// Removing spark info log
		import org.apache.log4j.{Level, Logger}
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		// Generating  SparkSession
		implicit val spark: SparkSession = SparkSession.builder()
			.master("local")
			.getOrCreate()

		// Generating input stream
		val stream = new SparkConsumer().readTopic("test") // TODO read from conf file

		// Proessing data TODO
		val query = stream.filter(!_.isNullAt(0)) // Remove rows with null id.

		// Writing to HDFS


		// Starts query..
		query
			.writeStream
			.format("console")
			.start()
			.awaitTermination()
	}


}
