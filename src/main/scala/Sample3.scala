import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Sample3 extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("AverageRating")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  // Input stream
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  case class Rating(name: String, rating: Option[Int])

  import scala.util.Try

  // Transformation
  val ratings = lines
    .as[String]
    .map(_.split(","))
    .filter(_.length == 2)
    .map { case Array(name, rating) =>
      Rating(name, Try(rating.toInt).toOption)
    }

  ratings.createOrReplaceTempView("ratings")

  // Output stream (watch the trigger & outputMode settings)
  spark
    .sql("SELECT name, AVG(rating) FROM ratings GROUP BY name")
    .writeStream
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .format("console")
    .outputMode("complete")
    .start()
    .awaitTermination()
}
