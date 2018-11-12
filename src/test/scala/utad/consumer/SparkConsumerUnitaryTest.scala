//package utad.consumer
//
//import net.manub.embeddedkafka.EmbeddedKafka
//import org.apache.spark.sql.{Dataset, SparkSession}
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//
//import scala.io.Source
//
//class SparkConsumerUnitaryTest extends FunSuite
//	with BeforeAndAfterAll
//	with EmbeddedKafka {
//
//	val filePath: String = "res/test/data/yellow_cabs.csv"
//	val topic: String = "consumer_topic"
//
//	var sparkSession: SparkSession = _
//
//	override def beforeAll(): Unit = {
//		EmbeddedKafka.start()
//
//		sparkSession = SparkSession.builder
//			.master("local")
//			.appName("test_consumer")
//			.getOrCreate()
//	}
//
//	override def afterAll(): Unit = {
//		EmbeddedKafka.stop()
//	}
//
//	test("Should read dataframe from Kafka") {
//		implicit val spark: SparkSession = sparkSession
//
//		val bufferedSource = Source.fromFile(filePath)
//		var header = 0
//		for (line <- bufferedSource.getLines) {
//			if (header > 1) {
//				EmbeddedKafka.publishStringMessageToKafka(topic, line)
//			} else {
//				header = header + 1 // file has 2 rows for none data
//			}
//		}
//
//		val consumer: SparkConsumer = new SparkConsumer()
//		val df: Dataset[String] = consumer.readTopic(topic)
//		df.show()
//
//
//	}
//
//}
