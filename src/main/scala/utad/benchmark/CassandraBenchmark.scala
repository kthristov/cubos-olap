package utad.benchmark

import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._

class CassandraBenchmark {

	// TODO

	val spark: SparkSession = SparkSession.builder()
		.config("spark.cassandra.connection.host", "localhost")
		.master("local")
		.getOrCreate()

	val df = spark
		.read
		.format("org.apache.spark.sql.cassandra")
		.options(Map( "table" -> "taxi", "keyspace" -> "olap" ))
		.load()

	df.show



}
