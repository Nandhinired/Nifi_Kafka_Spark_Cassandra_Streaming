import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{StructField, StructType}
import com.datastax.spark.connector
import org.apache.spark.sql.cassandra
import scala.concurrent.duration.DurationInt

object Nifi_Kafka_Spark_Cassandra {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("File Streaming Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()

     // Seamless Read from the Kafka Stream

    val kafkaInput = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "nifikafka")
      .option("startingOffsets", "earliest")
      .load()

    // Converting the serialized Bytes to string

    val kafkaValue = kafkaInput.select("value").withColumn("value", expr("cast(value as string)"))

    // Programmatically defining the schema for the json data

    val schema = StructType(Array(
      StructField("nationality", StringType, true),
      StructField("results", ArrayType(
        StructType(Array(
          StructField("user", StructType(Array(
            StructField("cell", StringType, true),
            StructField("dob", LongType, true),
            StructField("email", StringType, true),
            StructField("gender", StringType, true),
            StructField("location", StructType(Array(
              StructField("city", StringType, true),
              StructField("state", StringType, true),
              StructField("street", StringType, true),
              StructField("zip", LongType, true))), true),
            StructField("md5", StringType, true),
            StructField("name", StructType(Array(
              StructField("first", StringType, true),
              StructField("last", StringType, true),
              StructField("title", StringType, true))), true),
            StructField("password", StringType, true),
            StructField("phone", StringType, true),
            StructField("picture", StructType(Array(
              StructField("large", StringType, true),
              StructField("medium", StringType, true),
              StructField("thumbnail", StringType, true))), true),
            StructField("registered", LongType, true),
            StructField("salt", StringType, true),
            StructField("sha1", StringType, true),
            StructField("sha256", StringType, true),
            StructField("username", StringType, true))), true))), true), true),
      StructField("seed", StringType, true), StructField("version", StringType, true)))

    // Applying the schema onto the json field to create a dataframe

    val kafkaDf = kafkaValue.select(from_json(col("value"), schema).alias("details"))
      .select("details.*")

    // Exploding the Array fields

    val explodDf = kafkaDf.withColumn("results", explode(col("results")))
      .selectExpr("results.user.*", "nationality")

    // Selecting the required fields

    val structDf = explodDf.selectExpr("username", "dob", "email", "gender", "location.state as state",
      "name.first as first_name", "name.last as last_name", "phone", "nationality")
      .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
      .withColumn("dob", to_date(from_unixtime(col("dob"))))

    val caseDf = structDf.withColumn("gender", expr("case when gender = 'male' then 'M' else 'F' end"))
      .select(col("full_name").alias("name"), col("*")).drop("first_name", "last_name", "full_name")

    val regexDf = caseDf.withColumn("username", regexp_extract(col("username"), "[a-z]+", 0))

     // Writing to Cassandra

    regexDf.writeStream
      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
        batchDf.write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", "kafka_cassandra")
          .option("table", "kafka_write_cassandra")
          .mode("Append")
          .save()

      }
      .option("checkpointLocation", "file:///C:/Users/nandh/Desktop/chekpoint")
      .trigger(ProcessingTime(10.seconds))
      .queryName("Flattened Users Data ")
      .start()
      .awaitTermination()


  }


}
