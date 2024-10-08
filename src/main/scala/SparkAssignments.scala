


import io.netty.handler.codec.dns.DnsQuestion
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.zookeeper.Transaction

import scala.reflect.internal.util.NoPosition.show
import scala.reflect.internal.util.TriState.True



object SparkAssignments{
  def main(args: Array[String]): Unit = {
    //Logger.getLogger(("org")).setLevel(Level.OFF)
    //Logger.getLogger(("akka")).setLevel(Level.OFF)

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
//   val logins = List(
//     (1, "09:00"),
//     (2, "18:30"),
//     (3, "14:00")
//   ).toDF("login_id", "login_time")
//
//
//  logins.select(
//    col("login_id"),
//    col("login_time"),
//    when(to_timestamp (col("login_time"),"HH:mm")<to_timestamp(lit("12:00"),"HH:mm"),"true")
//      .otherwise("False")
//  ).show()

//    Question: How would you add a new column category with values "Young & Low Salary" if age is less
//    than 30 and salary is less than 35000, "Middle Aged & Medium Salary" if age is between 30 and 40
//    and salary is between 35000 and 45000, and "Old & High Salary" otherwise?


//    val employees = List(
//      (1, 25, 30000),
//      (2, 45, 50000),
//      (3, 35, 40000)
//    ).toDF("employee_id", "age", "salary")
//
//    employees.select(
//      col("employee_id"),
//       col( "age"),
//      col("salary"),
//      when (col("age")<30 && col("salary")<35000,"young and low salary")
//        .when (col("age")<40 && col("salary")<45000,"middle age & salary")
//        .otherwise("old and high")
//
//    ).show()

    ///spark sql example
//    val data=List(("mohan",56,100),("vijay",45,23),("ajay",67,68)).toDF("name","age","marks")
//
//
//    data.createOrReplaceTempView("customer")
//
//    spark.sql("""
//    SELECT name,
//           age,
//           marks,
//           CASE
//               WHEN marks > 50 THEN 'grade A'
//               WHEN marks BETWEEN 30 AND 50 THEN 'grade B'
//               ELSE 'fail'
//           END AS Grade
//    FROM customer
//""").show()


//    employee_id review_date performance_score review_text
//    val data1 = List(
//      (1, "2024-01-10", 8, "Good performance"),
//      (2, "2024-01-15", 9, "Excellent work!"),
//      (3, "2024-02-20", 6, "Needs improvement."),
//      (4, "2024-02-25", 7, "Good effort."),
//      (5, "2024-03-05", 10, "Outstanding!"),
//      (6, "2024-03-12", 5, "Needs improvement.")
//    ).toDF("employee_id","review_date","performance_score","review_text")
//
//    data1.select(
//      col("employee_id"),
//      col("review_date"),
//      col("performance_score"),
//      when (col("performance_score")>=9,"excellent")
//        .when(col("performance_score")>=7 && col("performance_score")<9,"great work")
//        .otherwise("Needs improvement")
//        .alias("performance")
//
//    ).filter(col("performance")==="excellent")
//      .show ()
//
//
//    data1.createOrReplaceTempView("data1performance")
//
//
//    spark.sql("""
//      select employee_id,
//      performance_score,
//      case  when performance_score >=9 then "excelent"
//            when (performance_score >=7 and performance_score <9 )then "great"
//            else ("needs impovement")
//            end as Grade
//
//      from data1performance
//
//
//      """).show()



    ////
    ///Finding the count of orders placed by each customer and the total order amount for
      //each customer
//    val orderData = Seq(
//      ("Order1", "John", 100),
//      ("Order2", "Alice", 200),
//      ("Order3", "Bob", 150),
//      ("Order4", "Alice", 300),
//      ("Order5", "Bob", 250),
//      ("Order6", "John", 400)
//    ).toDF("OrderID", "Customer", "Amount")
//
//    orderData.groupBy("Customer")
//      .agg(sum("Amount")
//      ,count("OrderId")
//      )
//      .show()

      //Finding the average score for each subject and the maximum score for each student
//      val scoreData = Seq(
//        ("Alice", "Math", 80),
//        ("Bob", "Math", 90),
//        ("Alice", "Science", 70),
//        ("Bob", "Science", 85),
//        ("Alice", "English", 75),
//        ("Bob", "English", 95)
//      ).toDF("Student", "Subject", "Score")
//
//    scoreData.groupBy("Student")
//      .agg(
//        avg("Score"),
//        max("Score")
//      ).show()


  //Finding the average rating for each movie and the total number of ratings for each movie.

//    val ratingsData = Seq(
//      ("User1", "Movie1", 4.5),
//      ("User2", "Movie1", 3.5),
//      ("User3", "Movie2", 2.5),
//      ("User4", "Movie2", 3.0),
//      ("User1", "Movie3", 5.0),
//      ("User2", "Movie3", 4.0)
//    ).toDF("User", "Movie", "Rating")
//
//
//    ratingsData.groupBy("Movie")
//      .agg(
//        avg(col("Rating")),
//        count("Rating")
//      ).show()

    //    val ddlschema="id Int,Name String,Salary Int,City String"

//    val pg1schema=StructType(List(
//      StructField("id",IntegerType),
//      StructField("Name",StringType),
//      StructField("Salary",IntegerType),
//      StructField("City",StringType)
//    ))
//
//    val df=spark.read
//      .format("csv")
//      .option("header",true)
//      .schema(pg1schema)
//      .option("mode","PERMISSIVE")
//      .option("path","C:/Users/padma/Downloads/details.csv")
//      .load()
//    df.write
//      .format("csv")
//      .option("header",true)
//      .mode(SaveMode.Ignore)
//      .option("path","C:/Users/padma/Downloads/sep26op")
//      .save()
//    df.show()
//  df.printSchema()


//    val salesData = Seq(
//      ("Product1", "Category1", 100),
//      ("Product2", "Category2", 200),
//      ("Product3", "Category1", 150),
//      ("Product4", "Category3", 300),
//      ("Product5", "Category2", 250),
//      ("Product6", "Category3", 180),
//      ("Product1", "Category2", 100),
//      ("Product2", "Category1", 200),
//      ("Product3", "Category2", 150),
//      ("Product4", "Category4", 300),
//      ("Product5", "Category1", 250),
//      ("Product6", "Category4", 180)
//    ).toDF("Product", "Category", "Revenue")
//
//
//
//    val windowSpec = Window.partitionBy("Product").orderBy("Category")
//
//    val runningTotal = salesData.withColumn("RunningTotal", sum("Revenue").over(windowSpec))
//    runningTotal.show()

    //apply lead on top of order_date......and get the answer
//    val d1 = Seq (
//      (101,"CustomerA","2023-09-01"),
//      (103,"CustomerA","2023-09-03"),
//      (102,"CustomerB","2023-09-02"),
//      (104,"CustomerB","2023-09-04")
//    ).toDF("order_id", "customer","order_date")
//
//    val window =Window.partitionBy("customer").orderBy("order_date")
//
//    val leaddf = d1.select(
//      col("order_id"),
//      col("customer"),
//      col("order_date"),
//      lead("order_date",1).over(window)
//    ).show()


    //we want to find the difference between the price on each day with it’s previous day.
//    val d2 = Seq(
//      (1, "KitKat",1000.0,"2021-01-01"),
//      (1, "KitKat",2000.0,"2021-01-02"),
//      (1, "KitKat",1000.0,"2021-01-03"),
//      (1, "KitKat",2000.0,"2021-01-04"),
//      (1, "KitKat",3000.0,"2021-01-05"),
//      (1, "KitKat",1000.0,"2021-01-06")
//
//    ).toDF("IT_ID","IT_Name","Price", "PriceDate")
//
//    val win =Window.partitionBy(col("IT_Name")).orderBy(col("PriceDate"))
//
//    val d2output = d2.select(
//      col("IT_ID"),
//      col("IT_Name"),
//      col("Price"),
//      col("PriceDate"),
//      col("Price")-lag("Price",1).over(win)
//    ).show()


//. If salary is less than previous month we will mark it as "DOWN", if salary has increased then "UP"
    //
    //ID,NAME,SALARY,DATE
    //1,John,1000,01/01/2016
    //1,John,2000,02/01/2016
    //1,John,1000,03/01/2016
    //1,John,2000,04/01/2016
    //1,John,3000,05/01/2016
    //1,John,1000,06/01/2016

//    val d3 = Seq(
//      (1,"John",1000,"01/01/2016"),
//      (1,"John",2000,"02/01/2016"),
//      (1,"John",1000,"03/01/2016"),
//      (1,"John",2000,"04/01/2016"),
//      (1,"John",3000,"05/01/2016"),
//      (1,"John",1000,"06/01/2016")
//    ).toDF("ID","NAME","SALARY","DATE")
//
//
//  val win= Window.orderBy(col("date"))
//
//
//    val d4 = d3.withColumn("new1",lead(col("SALARY"),1).over(win))
//
//    val d3output= d4.select(
//      col("ID"),
//      col("NAME"),
//      col("SALARY"),
//      col("DATE"),
//      col("new1"),
//      (when(col("SALARY")>col("new1"),"up")
//        .when(col("SALARY")<col("new1"),"down")
//        .otherwise("nochange"))
//    ).show()

//    d3output.withColumn("new1",(when(col("SALARY")>col("prevsal"),"up").otherwise("down")))


//    val d5 = List(("2024-09-28"),("2024-09-30"),(null)).toDF("datestring")

    //d5.withColumn("futuredate",when((col("datestring")).isNull,("na")).otherwise(date_add(col("datestring"),4))



//    d5.withColumn("futuredate",
//      when(col("datestring").isNull,lit ("na"))
//        .otherwise(date_add(to_date(col("datestring")), 4))).show()



//Given a DataFrame with date1 and date2 columns, handle missing date values
//by filling them with default dates.
//val df = List(("2023-10-07", null), (null, "2023-10-08")).toDF("date1","date2")
//
//   val df2= df
//     .withColumn("date1", when(col("date1").isNull, lit("Defaultdate1")).otherwise(col("date1")))
//     .withColumn("date2", when(col("date2").isNull, lit("Defaultdate2")).otherwise(col("date2")))
//
//    .show()


    //Finding the count of orders placed by each customer and the total order amount for
      //each customer.

//    val orderData = Seq(
//  ("Order1", "John", 100),
//  ("Order2", "Alice", 200),
//  ("Order3", "Bob", 150),
//  ("Order4", "Alice", 300),
//  ("Order5", "Bob", 250),
//  ("Order6", "John", 400)
//).toDF("OrderID", "Customer", "Amount")
//    orderData.show()
//
//
//    val aggregatedData = orderData.groupBy("Customer")
//      .agg(
//        count("OrderID").alias("OrderCount"),
//        sum("Amount").alias("TotalAmount")
//      )
//    aggregatedData.show()


  //  Finding the average score for each subject and the maximum score for each student.
//    val scoreData = Seq(
//      ("Alice", "Math", 80),
//      ("Bob", "Math", 90),
//      ("Alice", "Science", 70),
//      ("Bob", "Science", 85),
//      ("Alice", "English", 75),
//      ("Bob", "English", 95)
//    ).toDF("Student", "Subject", "Score")
//
//    val d1 = scoreData.groupBy("Subject")
//      .agg(
//        avg("Score").alias("avg"),
//        max("Score").alias("max")
//      )
//      d1.show()


    //Finding the average rating for each movie and the total number of ratings for each movie.

//    val ratingsData = Seq(
//      ("User1", "Movie1", 4.5),
//      ("User2", "Movie1", 3.5),
//      ("User3", "Movie2", 2.5),
//      ("User4", "Movie2", 3.0),
//      ("User1", "Movie3", 5.0),
//      ("User2", "Movie3", 4.0)
//    ).toDF("User", "Movie", "Rating")
//
//
//    ratingsData.groupBy("Movie")
//      .agg(
//        avg("Rating").alias("avg raing")
//
//
//      ).show()



//Finding the count of distinct products purchased by each customer and the total purchase
    //amount for each customer.


//    val purchaseData = Seq(
//      ("Customer1", "Product1", 100),
//      ("Customer1", "Product2", 150),
//      ("Customer1", "Product3", 200),
//      ("Customer2", "Product2", 120),
//      ("Customer2", "Product3", 180),
//      ("Customer3", "Product1", 80),
//      ("Customer3", "Product3", 250)
//    ).toDF("Customer", "Product", "Amount")
//
//
//        purchaseData.groupBy("Customer")
//          .agg(
//            countDistinct("Product").alias("distinct count"),
//            sum("Amount").alias("total purchase")
//
//          ).show()


    //Finding the cumulative sum of sales amount for each product.
//    val salesData = Seq(
//      ("Product1", 100),
//      ("Product2", 200),
//      ("Product3", 150),
//      ("Product4", 300),
//      ("Product5", 250),
//      ("Product6", 180)
//    ).toDF("Product", "SalesAmount")
//
//    val win =Window.orderBy("Product")
//
//    val d1 = salesData.withColumn("cumulative sales",sum("SalesAmount").over(win)).show()


    //Create a new column price_category based on price:
//    val df = List(
//      (1, "Smartphone" ,700, "Electronics"),
//      (2, "TV" ,1200, "Electronics"),
//      (3, "Shoes" ,150, "Apparel"),
//    (4, "Socks" ,25, "Apparel"),
//    (5, "Laptop" ,800, "Electronics"),
//    (6, "Jacket", 200, "Apparel")
//    ).toDF("product_id", "product_name", "price","category")
//
//    val df2 =df.withColumn(
//      "price_type",
//      when(col("price")>500,"Expensive")
//        .when(col("price")>=200,"moderate")
//        otherwise("cheap")
//    ).filter(col("product_name").like("S%") or (col("product_name").like("%S")))
//
//
//    val df3 = df.groupBy("category")
//      .agg(
//        sum(col("price")),
//        avg(col("price")),
//        max(col("price")),
//        min(col("price"))
//      ).show()


    //3 Create a new column rating_category based on rating:
    //o
    //'Excellent' if rating >= 8
    //o
    //'Good' if 6 <= rating < 8
    //o
    //'Average' if rating < 6
//    val movies = List(
//      (1, "The Matrix", 9, 136),
//      (2, "Inception", 8, 148),
//      (3, "The Godfather", 9, 175),
//      (4, "Toy Story", 7, 81),
//      (5, "The Shawshank Redemption", 10, 142),
//      (6, "The Silence of the Lambs", 4, 118)
//    ).toDF("movie_id", "movie_name",
//      "rating", "duration_minutes")
//
//
//    val df1 = movies.withColumn("rating_Category",
//      when(col("rating")>=8,"excellent")
//        .when(col("rating")>=6,"good").otherwise("avg")
//    ).show()
//
//  }

    //Create a new column transaction_month that extracts the month from transaction_date.
    //
    //Filter transactions that occurred in the month of 'December'.

//    val transactions = List(
//      (1, "2023-12-01", 1200, "Credit"),
//      (2, "2023-11-15", 600, "Debit"),
//      (3, "2023-12-20", 300, "Credit"),
//      (4, "2023-10-10", 1500, "Debit"),
//     (5, "2023-12-30", 250, "Credit"),
//      (6, "2023-09-25", 700, "Debit")
//    ).toDF("transaction_id", "transaction_date", "amount", "transaction_type")
//
//
//
//
//    val df2 = transactions.withColumn("moth",
//      month(col("transaction_date"))
//
//    ).filter(col("moth")==="12").show()

    //Create a new column rating_category based on rating:
    //o
    //'Excellent' if rating >= 5
    //o
    //'Good' if 3 <= rating < 5
    //o
    //'Poor' if rating < 3
    //
    //Filter feedback with feedback_text that starts with 'Great'.
    //
    //Calculate the average rating per month.

//    val reviews = List(
//(1, "2024-01-10", 4, "Great service!"),
//(2, "2024-01-15", 5, "Excellent!"),
//(3, "2024-02-20", 2, "Poor experience."),
// (4, "2024-02-25", 3, "Good value."),
//(5, "2024-03-05", 4, "Great quality."),
//(6, "2024-03-12", 1, "Bad service.")
//).toDF("customer_id", "feedback_date", "rating", "feedback_text")
//
//
////    reviews.withColumn("ratings_new",
////      when(col("rating")>=5,"excellent")
////        .when(col("rating")>3,"good")
////        .otherwise("poor")
////
////        ).show()
//
//
//    // Create a DataFrame from the sequence
//    val df = reviews.toDF()
//
//    // Group by feedback_date and show the results
//
//
//    df.groupBy(month(col("feedback_date")).alias("month"))
//      .agg(avg(col("rating")).alias("average_rating"))
//      .show()


///Calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount
//for each performance category
//    val salesRecords = Seq(
//      (1, "North-West", 12000, "2024-01-10"),
//      (2, "South-East", 6000, "2024-01-15"),
//    (3, "East-Central", 4000, "2024-02-20"),
//     (4, "West", 15000, "2024-02-25"),
//     (5, "North-East", 3000, "2024-03-05"),
//      (6, "South-West", 7000, "2024-03-12")
//    ).toDF("sales_id" ,"region","sales_amount" ,"sales_date")
//
//
//    //salesRecords.filter(col("region").like("%West")).show()
//
//
//
//
//      val df2 = salesRecords.agg(max("sales_amount")).show()


//    salesRecords.withColumn("west_region",
//      when(col("region").like("%west"))


    //Filter salaries where department starts with 'IT'.
    //
    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) salary for each
    //salary_band
//    val employees = Seq(
//      (1, "IT", 130000, "2024-01-10"),
//      (2, "HR", 80000, "2024-01-15"),
//      (3, "IT", 60000, "2024-02-20"),
//      (4, "IT", 70000, "2024-02-25"),
//      (5, "Sales", 50000, "2024-03-05"),
//      (6, "IT", 90000, "2024-03-12")
//    ).toDF("employee_id", "department" ,"salary", "last_increment_date")
//
//
//    employees.withColumn("type",
//      when(col("salary")>120000,"high")
//        .when(col("salary")>60000,"medium")
//        .otherwise("low")
//    ).filter(col("department").like("IT%")).show()
//    val df1=spark.read
//      .format("csv")
//      .option("header",true)
//      .option("path","C:/Users/padma/Downloads/new1.csv")
//      .load()



    val df2 = spark.read
      .format("csv")
      .option("header", true)
      .option("path", "C:/Users/padma/Downloads/details.csv")
      .load()

    df2.show()
    df2.printSchema()

//    val condition=df1("id")===df2("id")
//
//    val jointype="fullouter"
//
//
//    val joineddf=df1.join(df2,condition,jointype).drop(df2("id"))
//
//    joineddf.show()


    df2.show()
//    df1.write
//      .format("csv")
//      .partitionBy("city")
//      .option("maxRecordsPerFile",2)
//      .mode(SaveMode.Overwrite)
//      .option("path","C:/Users/padma/Downloads/oct4op")
//      .save()






  }
}

