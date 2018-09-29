package utad.producer

import org.apache.spark.sql.SparkSession

case class Record (
	                  key : String,
	                  value: String
                  )

/**
  * Simple class when executed feeds a kafka topic with data read from given csv file.
  */
class DataProducer {

	val spark = SparkSession.builder.master("local").getOrCreate()

	import spark.implicits._

	def fillTopic(filePath: String, topic : String): Unit ={
		val df = spark.read
			.option("header", "true")
			.csv(filePath)

		val df_ser = df.map( row =>
			new Record(
				key = row.getAs[String]("VendorID") + row.getAs[String]("tpep_pickup_datetime"),
				value = row.mkString(",")
			)
		)

		df_ser.show()

		df_ser
		.write
		.format("kafka")
		.option("kafka.bootstrap.servers", "localhost:9092")
		.option("topic", topic)
		.save()


	}

}


