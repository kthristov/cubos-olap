package utad.producer

import org.apache.spark.sql.SparkSession


/**
  * Simple class when executed feeds a kafka topic with data read from given csv file.
  */
class DataProducer {


	def fillTopic(filePath: String, topic: String)(implicit spark: SparkSession) : Unit = {

		import spark.implicits._

		val df = spark.read
			.option("header", "true")
			.csv(filePath)

		val df_ser = df.map(row =>
			new Record(
				key = row.getAs[String]("VendorID") + row.getAs[String]("tpep_pickup_datetime"),
				value = row.mkString(",")
			)
		)

		df_ser
			.write
			.format("kafka")
			.option("kafka.bootstrap.servers", "localhost:6001") // TODO coger de conf
			.option("topic", topic)
			.save()

	}
}


case class Record(
	                 key: String,
	                 value: String
                 )

