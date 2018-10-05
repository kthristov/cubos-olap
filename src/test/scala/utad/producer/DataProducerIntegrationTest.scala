package utad.producer

import com.holdenkarau.spark.testing.SharedSparkContext
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.io.Source


class DataProducerIntegrationTest extends FunSuite
	with SharedSparkContext
	with BeforeAndAfterAll
	with EmbeddedKafka {

	val filePath = "res/test/data/yellow_cabs.csv"
	val topic = "producer_topic"


	override def beforeAll: Unit = {
		EmbeddedKafka.start()

		implicit val spark: SparkSession = SparkSession.builder
			.master("local")
			.appName("test_import")
			.getOrCreate()
	}

	override def afterAll(): Unit = {
		EmbeddedKafka.stop()
	}


	test("Should write to kafka") {

		val producer = new DataProducer()
		producer.fillTopic(filePath, topic)

		val bufferedSource = Source.fromFile(filePath)
		var header = 0
		for (line <- bufferedSource.getLines) {
			if (header > 1) {
				// TODO. Very slow, fix...
				assert(line == consumeFirstStringMessageFrom(topic))
			} else {
				header = header + 1 // file has 2 rows for none data
			}

		}

		bufferedSource.close
	}

}
