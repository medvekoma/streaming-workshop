import org.apache.spark.sql.SparkSession

object One {

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  val lines = spark.readStream
    .format("socket")
    .option("host","localhost")
    .option("port","9999")
    .load()

  case class Rating(name: String, rating: Option[Int])

  import scala.util.Try

  val ratings = lines
    .as[String]
    .map(_.split(","))
    .filter(_.size == 2)
    .map { case Array(name, rating) => Rating(name, Try(rating.toInt).toOption) }

  ratings.createOrReplaceTempView("ratings")

  spark.sql("SELECT name, AVG(rating) FROM ratings GROUP BY name")
    .writeStream
    .format("console")
    .outputMode("update")
    .start()
    .awaitTermination()

}
