# Using Spark Structured Streaming with Spark SQL

This example calculates the average rating of movies. It processes comma separated lines in the form of
```text
Movie title,<integer rating>
```

Use the same window setup as in the previous examples.

```bash
# Left window: Start a socket connection on port 9999.
nc -kl 9999

# Right window: Start the Spark command-line interface
spark-shell
```

Add the following code to the right window:

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

Start typing comma separated movie and rating lines in the left window.

```text
Interstellar,8
Tenet,7
Interstellar,9
Inception,10
Inception,9
Tenet,6
Better Call Saul,9
Inception,10
Better Call Saul,10
```

Check the output on the right

Check Spark UI
