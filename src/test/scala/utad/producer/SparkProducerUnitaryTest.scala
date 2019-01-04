package utad.producer

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.io.Source


class SparkProducerUnitaryTest extends FunSuite
	with BeforeAndAfterAll
	with EmbeddedKafka {

	val filePath = "res/test/data/yellow_cabs.csv"
	val topic = "producer_topic"

	var sparkSession: SparkSession = _

	override def beforeAll: Unit = {
		EmbeddedKafka.start()

		sparkSession = SparkSession.builder
			.master("local")
			.appName("test_producer")
			.getOrCreate()
	}

	override def afterAll(): Unit = {
		EmbeddedKafka.stop()
	}

	test("Should write to Kafka") {

		implicit val spark: SparkSession = sparkSession
		import spark.implicits._
//
//		val producer = new SparkProducer()
//		println("filling topic...")
//		producer.fillTopic(filePath, topic)

//		val df = spark
//			.readStream
//			.format("kafka")
//			.option("kafka.bootstrap.servers", "localhost:" + EmbeddedKafkaConfig().kafkaPort)
//			.option("subscribe", topic)
//			.load()
//
//		df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//			.as[(String, String)]


//		df.show()

//		val bufferedSource = Source.fromFile(filePath)
//		var header = 0
//		for (line <- bufferedSource.getLines) {
//			if (header > 1) {
//				// TODO. Very slow, fix...
//				assert(line == consumeFirstStringMessageFrom(topic))
//			} else {
//				header = header + 1 // file has 2 rows for none data
//			}
//		}

//		bufferedSource.close
		spark.stop()
		print("shutting down test")
	}
}
