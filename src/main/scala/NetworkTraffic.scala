import org.apache.spark.sql.functions.{col, sum, to_timestamp, window}
import org.apache.spark.sql.{Dataset, SparkSession}

object NetworkTraffic extends App {

  // sudo tcpdump -i en0 -n -tttt | nc -kl 9999
  val trafficPattern = "^([0-9-]+ [0-9:.]+) IP ([0-9.]+) > ([0-9.]+).+length ([0-9]+)".r

  case class Traffic(
      timeStamp: String,
      source: String,
      target: String,
      length: Long
  )

  def parseTraffic: String => Option[Traffic] = {
    case trafficPattern(ts, source, target, length) =>
      Some(Traffic(ts, source, target, length.toLong))
    case _ => None
  }

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("AverageRating")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  // Input stream
  val traffic: Dataset[Traffic] = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()
    .as[String]
    .flatMap(parseTraffic)

  traffic.printSchema()

  val traffic2 = traffic
    .withColumn("ts", to_timestamp($"timestamp"))
    .groupBy(window($"ts", "10 seconds", "5 seconds"), $"source")
    .agg(sum($"length").as("total"))
//    .orderBy($"window.start")
    .filter($"total" > 100)

  traffic2.printSchema()

  traffic2.writeStream
    .format("console")
    .option("truncate", value = false)
    .outputMode("update")
    .start()
    .awaitTermination()
}
