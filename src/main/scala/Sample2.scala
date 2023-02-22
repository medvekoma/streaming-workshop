import org.apache.spark.sql.SparkSession

object Sample2 extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("AverageRating")
    .getOrCreate()

  // disable debug logs
  spark.sparkContext.setLogLevel("ERROR")

  val dict = spark.read
    .option("header", value = true)
    .csv("dictionary.csv")

  // Create the streaming DataFrame
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  // Create the transformation
  val translated = lines
    .join(dict, lines("value") === dict("English"), "leftouter")

  // You can check the Spark UI to see that nothing happens

  // Create the output stream
  translated.writeStream
    .format("console")
    .outputMode("update")
    .start()
    .awaitTermination()
}
