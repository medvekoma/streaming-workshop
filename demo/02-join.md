# Using Spark Structured Streaming Joins

This example implements a simple transformation based on a static dictionary.

Use the same terminal structure as in the first sample.

Execute the following commands in the left window:

```bash
# Left window: Create a text file
cat > dictionary.csv

# Paste in the following lines. Exit with Ctrl+C
English,German
one,Eins
two,Zwei
three,Drei
four,Vier

# Start the new socket connection
nc -kl 9999
```

Start the spark application in the right window

```bash
# Right window: start Spark shell
spark-shell
```

```scala
// Create a static DataFrame
val dict = spark.read
  .option("header", true)
  .csv("dictionary.csv")

// disable debug logs
spark.sparkContext.setLogLevel("ERROR")

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
val query = translated.writeStream
  .format("console")
  .outputMode("update")
  .start()
  .awaitTermination()
```

Start typing lines to the left window. Use the words `one`, `two`, `three`, and `four`.

Check Spark UI
