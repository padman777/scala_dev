import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

object setA150Questions {
  def main(args: Array[String]): Unit = {

 val spark = SparkSession.builder()
   .master("local[*]")
   .getOrCreate()
//question 1
    //Calculate the average score per subject.
    //
    //Find the maximum and minimum score per subject.
    //
    //Count the number of students in each grade category per subject.
    //Create a new column grade based on the score:
    //o
    //'A' if score >= 90
    //o
    //'B' if 80 <= score < 90
    //o
    //'C' if 70 <= score < 80
    //o
    //'D' if 60 <= score < 70
    //o
    //'F' if score < 60

    import spark.implicits._
    val sampledata = Seq(
    ("Alice", 92 ,"Math"),
    ("Bob" ,85, "Math"),
    ("Carol", 77 ,"Science"),
    ("Dave", 65 ,"Science"),
    ("Eve", 50, "Math"), ("Frank", 82, "Science")
    ).toDF("Stuent","Score","Subject")


//    sampledata.groupBy(col("Subject"))
//      .agg(
//          avg(col("score"))
//
//      ).show()

//    val gradedData = sampledata.withColumn("grade",
//      when (col("Score")>90,"A")
//        .when(col("Score")>70,"c")
//        .otherwise("B")
//    ).show  ()
//      sampledata.groupBy(col("subject"))
//        .agg (
//          min("score") alias  ("minscore"),
//          max("score") alias("max score")
//
//
//        ).show()





  }

}
