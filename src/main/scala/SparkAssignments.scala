


import io.netty.handler.codec.dns.DnsQuestion
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

import scala.reflect.internal.util.NoPosition.show
import scala.reflect.internal.util.TriState.True



object SparkAssignments{
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

//    val data= List(("mohan", 56, 100), ("vijay", 45, 23), ("ajay", 67, 68)).toDF("name", "age", "marks")


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

    //data.show()

    //scala.io.StdIn.readLine()

    // data.select(
    //   col("name"),
    //   col("age"),
    //   when(col("age") > 55 && col("name").endsWith("n"), "Senior").otherwise("junior").alias("condition")
    // ).show()


    // data.filter(col("age") > 55).show()
 //// WHEN AND OTHERWISE DnsQuestion

    // QUESTION 1

//    import spark.implicits._
//    val employees = List(
//      (1, "John", 28),
//      (2, "Jane", 35),
//      (3, "Doe", 22)
//    ).toDF("id", "name", "age")
//
//    employees.select(
////      col("id"),
////      col("name"),
////      col("age"),
//      *,
//      when(col("age")>=18,"adult").otherwise("minor").alias("adultornot")



//    ).show()


    //question 3

//    import spark.implicits._
//
//    val transactions = List(
//      (1, 1000),
//      (2, 200),
//      (3, 5000)
//    ).toDF("transaction_id", "amount")
//
//    transactions.select(
//      col("transaction_id"),
//      col("amount"),
//      when(col("amount")>1000,"high").when(col("amount")>500 && col("amount")<=1000,"medium").otherwise("low")
//
//    ).show()


    //Question: How would you add a new column is_holiday which is true if the date is "2024-12-25" or
    //      "2025-01-01", and false otherwise?

//    import spark.implicits._
//    val events = List(
//      (1, "2024-07-27"),
//      (2, "2024-12-25"),
//      (3, "2025-01-01")
//    ).toDF("event_id", "date")
//
//    events.select(
//      col("event_id"),
//      col("date"),
//      when(col("date")==="2024-12-25","true").otherwise("false").alias("is_holiday")
//    ).show()

  //How would you add a new column stock_level with values "Low" if quantity is less than 10,
    //"Medium" if quantity is between 10 and 20, and "High" otherwise?

    import spark.implicits._

//    val inventory = List(
//      (1, 5),
//      (2, 15),
//      (3, 25)
//    ).toDF("item_id", "quantity")
//
//    inventory.select(
//      col("item_id"),
//      col("quantity"),
//      when (col("quantity")<10,"low").when(col("quantity")>10 && col("quantity")<20,"med").otherwise("high").alias("type")
//    ).show()


      //Question: How would you add a new column email_provider with values "Gmail" if email contains
    //"gmail", "Yahoo" if email contains "yahoo", and "Other" otherwise?

//      val customers = List(
//        (1, "john@gmail.com"),
//        (2, "jane@yahoo.com"),
//        (3, "doe@hotmail.com")
//      ).toDF("customer_id", "email")
//
//    customers.select(
//      col("customer_id"),
//      col("email"),
//      when (col("email").contains("gmail"),"Gmail").otherwise("others").as("gmailcontaining")
//    ).show()


    //Question: How would you add a new column season with values "Summer" if order_date is in June,
    //July, or August, "Winter" if in December, January, or February, and "Other" otherwise?
//    val orders = List(
//      (1, "2024-07-01"),
//      (2, "2024-12-01"),
//      (3, "2024-05-01"),
//       (4, "2024-06-01")
//    ).toDF("order_id", "order_date")
//
//    orders.select(
//      col("order_id"),
//      col("order_date"),
//      when(date_format(col("order_date"),"LLL").isin("Jan","Feb","Dec"),"Winter")
//        .when(date_format(col("order_date"),"LLL").isin("Jun","Jul","Aug"),"Summer")
//        .otherwise("Other").alias("Others")
//    ).show()


   // How would you add a new column is_morning which is true if login_time is before 12:00,
    //and false otherwise?
   val logins = List(
     (1, "09:00"),
     (2, "18:30"),
     (3, "14:00")
   ).toDF("login_id", "login_time")





  }




}

