package examples

import java.io.{File, FileWriter}
import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordsAnalytics extends App {

  /**
    *
    * How to run on Idea:
    *
    * 1. Wait for sbt to resolve all dependencies
    * 2. Click on the play symbol on the object declaration line
    *
    * How to run in sbt shell:
    *
    * 1. Open the sbt shell
    * 2. Replace the command below with the appropriate path and run it
    *
    * $ runMain examples.WordsAnalytics <Path to project folder>/src/main/resources/poem_1.txt C:/temp/results/1 C:/temp/results/2
    */
  println("### Running")
  if (args.length < 3) {
    println("Arguments expected: input-text-file.txt output-folder-path-1 output-folder-path-2")
    System.exit(1)
  }

  val textFile = args(0)
  val outputPath1 = args(1)
  val outputPath2 = args(2)
  println("### Args received / " + textFile + " / " + outputPath1)

  // Spark code
  val conf = new SparkConf()
    .setAppName("Poem word count")
    .setMaster("local[1]")
    .set("spark.hadoop.validateOutputSpecs", "false")
  val sc = new SparkContext(conf)

  val baseRDD:RDD[String] = sc.textFile(textFile)

  //=================================================== Start of the Part 1

  //  1. Count all the words
  println("### 1. Count all the words")
  val allWords = baseRDD.flatMap(_.split(" "))
    //.map(w => w.toLowerCase().replaceAll("[^a-z|\n\r]",""))
    .map(w => w.toLowerCase().replaceAll("[\\W]",""))
    .filter(w => w.length > 0)
    .cache()

  writeToFile(Paths.get(outputPath1,"words-count.txt").toString, "Number of words: " + allWords.count())
  println("###### Number of words: " + allWords.count())

  //  2. Count repeating words (frequency of each word)
  println("### 2. Count repeating words")
  val aggregateWords = allWords
    .map(w => (w, 1) )
    .reduceByKey(_+_)
    .cache()

  aggregateWords.saveAsTextFile(Paths.get(outputPath1,"words-frequency").toString)

  //  3. Find out the longest word
  println("### 3. Find out the longest word")

  val longestKey = aggregateWords.sortBy(t => t._1.length, ascending = false)
    .first()._1

  aggregateWords.filter(_._1 == longestKey)
    .saveAsTextFile(Paths.get(outputPath1,"longest-word").toString)

  println("###### Longest word: " + longestKey)

  //  4. Find out the most frequent word (most repetitive)
  println("### 4. Find out the most frequent word")
  val mostRepeated = aggregateWords.map(_.swap)
    .sortByKey(ascending = false)
  val firstKey = mostRepeated.first()._2

  mostRepeated.filter(_._2 == firstKey)
    .saveAsTextFile(Paths.get(outputPath1,"most-frequent-word").toString)

  println("###### Most common word: " + firstKey)

  //  5. Order the words by their length ascending
  println("### 5. Order the words by their length ascending")
  val wordsByLengthAsc = aggregateWords.map(_._1).sortBy(w => w.length)
  wordsByLengthAsc.saveAsTextFile(Paths.get(outputPath1,"words-by-length-ascending").toString)

  //  6. Order the words by their frequency (count) ascending
  println("### 6. Order the words by their frequency (count) ascending")
  val wordsByFreqAsc = aggregateWords.map(_.swap).sortByKey().map(_.swap)
  wordsByFreqAsc.saveAsTextFile(Paths.get(outputPath1,"words-by-freq-ascending").toString)

  //  7. Order the words alphabetically ascending
  println("### 7. Order the words alphabetically ascending")
  val wordsByAlphabetAsc = aggregateWords.map(_._1).sortBy(w => w)
  wordsByAlphabetAsc.saveAsTextFile(Paths.get(outputPath1,"words-by-alphabet-ascending").toString)

  //  8. Order the words by their length descending
  println("### 8. Order the words by their length descending")
  val wordsByLengthDes = aggregateWords.map(_._1).sortBy(w => w.length, ascending = false)
  wordsByLengthDes.saveAsTextFile(Paths.get(outputPath1,"words-by-length-descending").toString)

  //  9. Order the words by their frequency (count) descending
  println("### 9. Order the words by their frequency (count) descending")
  val wordsByFreqDes = aggregateWords.map(_.swap).sortByKey(ascending = false).map(_.swap)
  wordsByFreqDes.saveAsTextFile(Paths.get(outputPath1,"words-by-freq-descending").toString)

  //  10. Order the words alphabetically descending
  println("### 10. Order the words alphabetically descending")
  val wordsByAlphabetDes = aggregateWords.map(_._1).sortBy(w => w, ascending = false)
  wordsByAlphabetDes.saveAsTextFile(Paths.get(outputPath1,"words-by-alphabet-descending").toString)

  //  11. Find out all words having frequency 1
  println("### 11. Find out all words having frequency 1")
  aggregateWords.filter(_._2 == 1)
    .saveAsTextFile(Paths.get(outputPath1,"words-with-freq-1").toString)

  //  12. Find out all words having frequency 2
  println("### 12. Find out all words having frequency 2")
  aggregateWords.filter(_._2 == 2)
    .saveAsTextFile(Paths.get(outputPath1,"words-with-freq-2").toString)

  //=================================================== End of the Part 1

  //=================================================== Start of the Part 2

  //  13. Find all words starting with a
  println("### 13. Find all words starting with a")
  aggregateWords.filter(w => w._1.toLowerCase().startsWith("a"))
    .saveAsTextFile(Paths.get(outputPath2,"words-starting-a").toString)

  //  14. Find all words starting with b
  println("### 14. Find all words starting with b")
  aggregateWords.filter(w => w._1.toLowerCase().startsWith("b"))
    .saveAsTextFile(Paths.get(outputPath2,"words-starting-b").toString)

  //  15. Find all words containing a
  println("### 15. Find all words containing a")
  aggregateWords.filter(w => w._1.toLowerCase().contains('a'))
    .saveAsTextFile(Paths.get(outputPath2,"words-contains-a").toString)

  //  16. Find all words containing b
  println("### 16. Find all words containing b")
  aggregateWords.filter(w => w._1.toLowerCase().contains('b'))
    .saveAsTextFile(Paths.get(outputPath2,"words-contains-b").toString)

  //  17. Save words in reverse order
  println("### 17. Save words in reverse order")
  aggregateWords.map(w => (w._1.reverse,w._2) )
    .saveAsTextFile(Paths.get(outputPath2,"words-in-reverse").toString)

  //  18. Find all words ending in a
  println("### 18. Find all words ending in a")
  aggregateWords.filter(w => w._1.toLowerCase.endsWith("a"))
    .saveAsTextFile(Paths.get(outputPath2,"words-ending-a").toString)

  //  19. Find all words ending in b
  println("### 19. Find all words ending in b")
  aggregateWords.filter(w => w._1.toLowerCase.endsWith("b"))
    .saveAsTextFile(Paths.get(outputPath2,"words-ending-b").toString)

  //  20. Save all words upper case
  println("### 20. Save all words upper case")
  aggregateWords.map(w => (w._1.toUpperCase(),w._2) )
    .saveAsTextFile(Paths.get(outputPath2,"words-upper-case").toString)

  //  21. Save all words lower case
  println("### 21. Save all words lower case")
  aggregateWords.map(w => (w._1.toLowerCase,w._2) )
    .saveAsTextFile(Paths.get(outputPath2,"words-lower-case").toString)

  //  22. Save all words by capitalizing their first letter(see String API method called: capitalize())
  println("### 22. Save all words by capitalizing their first letter")
  aggregateWords.map(w => (w._1.capitalize, w._2) )
    .saveAsTextFile(Paths.get(outputPath2,"words-capitalized").toString)

  //  23. For each word, replace string “a” with “a1” (see String API, method called: replace())
  println("### 23. For each word, replace string \"a\" with \"a1\"")
  aggregateWords.map(w => (w._1.toLowerCase.replaceAll("a", "a1"), w._2) )
    .saveAsTextFile(Paths.get(outputPath2,"words-replaced-a-1").toString)

  //  24. For each word, replace string "a" with "a2" (see String API, method called: replace())
  println("### 24. For each word, replace string \"a\" with \"a2\"")
  aggregateWords.map(w => (w._1.toLowerCase.replaceAll("a", "a2"), w._2) )
    .saveAsTextFile(Paths.get(outputPath2,"words-replaced-a-2").toString)

  //=================================================== End of the Part 2

  sc.stop()

  println("### Finished")

  //=================================================== Helper methods

  /**
    * Used for reading/writing to database, files, etc.
    * Code From the book "Beginning Scala"
    * http://www.amazon.com/Beginning-Scala-David-Pollak/dp/1430219890
    */
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def writeToFile(fileName:String, data:String) = {
    println("### " + fileName)
    val f = new File(fileName)
    if (f.exists()) {
      f.delete()
    } else {
      f.getParentFile().mkdirs()
      f.createNewFile()
    }
    using(new FileWriter(fileName)) {
      fileWriter => fileWriter.write(data)
    }
  }
}


