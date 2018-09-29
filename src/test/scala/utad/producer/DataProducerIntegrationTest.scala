package utad.producer

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.streaming.kafka.KafkaTestUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}


class DataProducerIntegrationTest extends FunSuite
	with SharedSparkContext
	with BeforeAndAfterAll
{

	val filePath = "res/test/data/yellow_cabs.csv"
	val topic = "producer_topic"
	private var kafkaTestUtils: KafkaTestUtils = _


    override def beforeAll: Unit = {
	    kafkaTestUtils = new KafkaTestUtils
	    kafkaTestUtils.setup()
	    kafkaTestUtils.createTopic(topic)
    }

	override def afterAll(): Unit = if (kafkaTestUtils != null) {
		kafkaTestUtils.teardown()
		kafkaTestUtils = null
	}


	test("Should write to kafka") {
		val producer = new DataProducer()
		producer.fillTopic(filePath, topic)




	}

}
