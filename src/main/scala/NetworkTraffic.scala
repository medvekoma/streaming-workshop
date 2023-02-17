import frameless.TypedDataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, to_timestamp, window}
import org.apache.spark.sql.types._

object NetworkTraffic extends App {

  // sudo tcpdump -i en0 -n -tttt | nc -kl 9999
  val trafficPattern = "^([0-9-]+ [0-9:.]+) IP ([0-9.]+) > ([0-9.]+).+length ([0-9]+)".r

  case class Traffic(timeStamp: String, source: String, target: String, length: Int)


  def parseTraffic: String => Option[Traffic] = {
    case trafficPattern(ts, source, target, length) => Some(Traffic(ts, source, target, length.toInt))
    case _ => None
  }

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("AverageRating")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  // Input stream
  val traffic = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()
    .as[String]
    .flatMap(parseTraffic)

  traffic.printSchema()

  //  traffic.writeStream
  //    .format("console")
  //    .outputMode("update")
  //    .start()
  //    .awaitTermination()
  //
  val traffic2 = traffic
    .withColumn("ts", to_timestamp(col("timestamp")))
    .groupBy(window(col("ts"), "10 seconds", "5 seconds"), col("target"))
    .agg(sum(col("length")).as("total"))
    .orderBy(col("window.start"))

  traffic2.printSchema()

  traffic2.writeStream
    .format("console")
    .outputMode("complete")
    .start()
    .awaitTermination()


  //  traffic.createOrReplaceTempView("traffic")
  //
  //  spark.sql("SELECT target, SUM(length) as total FROM traffic GROUP BY target ORDER BY total DESC")
  //    .writeStream
  ////    .trigger(Trigger.ProcessingTime("5 seconds"))
  //    .format("console")
  //    .outputMode("complete")
  //    .start()
  //    .awaitTermination()
}
