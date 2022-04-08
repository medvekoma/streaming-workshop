# Using Spark Structured Streaming for WordCount

Create a text file
```bash
cat > dictionary.csv
```
Paste the following block
```text
English,German
one,Eins
two,Zwei
three,Drei
four,Vier
```

Create a static DataFrame

```scala
val dict = spark.read
  .option("header", true)
  .csv("dictionary.csv")
```

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
  
val translated = lines
  .join(dict, lines("value") === dict("English"), "leftouter")
  
val query = translated.writeStream
  .format("console")
  .outputMode("update")
  .start()
  .awaitTermination()
```

Check Spark UI