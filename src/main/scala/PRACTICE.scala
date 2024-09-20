

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

import scala.reflect.internal.util.NoPosition.show
import scala.reflect.internal.util.TriState.True



object PRACTICE{
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local[*]")
      .getOrCreate()

  import spark.implicits._
     val data= List(("mohan", 56, 100), ("vijay", 45, 23), ("ajay", 67, 68)).toDF("name", "age", "marks")


//     val df = spark.read
//       .format("csv")
//       .option("header",true)
//       .option("path","C:/Users/padma/Downloads/details.csv")
//       .load()
//    df.select(
//      col("id"),
//      col("salary"),
//      when(col("salary")>500,"rich").otherwise("poor").alias("tellme")
//
//
//    ).show()

    data.show()

    //scala.io.StdIn.readLine()

    // data.select(
    //   col("name"),
    //   col("age"),
    //   when(col("age") > 55 && col("name").endsWith("n"), "Senior").otherwise("junior").alias("condition")
    // ).show()


   // data.filter(col("age") > 55).show()

  }




}

