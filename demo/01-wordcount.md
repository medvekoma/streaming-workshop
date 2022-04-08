# Using Spark Structured Streaming for WordCount

Using a socket connection

```bash
nc -kl 9999
```

```scala
spark.sparkContext.setLogLevel("ERROR")

val lines = spark.readStream
  .format("socket")
  .option("host","localhost")
  .option("port","9999")
  .load()
  
// Check if we have a streaming DataFrame
lines.isStreaming

val wordCounts = lines
  .as[String]
  .flatMap(_.split(" "))
  .groupBy("value")
  .count()
  
wordCounts.isSteaming

val query = wordCounts.writeStream
  .format("console")
  .outputMode("update")
  .start()
  .awaitTermination()
```

Check Spark UI