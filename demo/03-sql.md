# Using Spark Structured Streaming for WordCount

## Creating a socket

```bash
nc -kl 9999
```

## Creating the spark application

Use a split window

```bash
spark-shell
```

```scala
// Disable debug lines
spark.sparkContext.setLogLevel("ERROR")

// Create input stream from localhost:9999
val lines = spark.readStream
  .format("socket")
  .option("host","localhost")
  .option("port","9999")
  .load()

// Define data model
case class Rating(name: String, rating: Option[Int])

import scala.util.Try

// Transform to data model
val ratings = lines
  .as[String]
  .map(_.split(","))
  .filter(_.size == 2)
  .map { case Array(name, rating) => Rating(name, Try(rating.toInt).toOption) }

// Register as SQL view
ratings.createOrReplaceTempView("ratings")

// Start streaming
spark.sql("SELECT name, avg(rating) FROM ratings group by name")
  .writeStream
  .format("console")
  .outputMode("update")
  .start()
  .awaitTermination()
```

Check Spark UI