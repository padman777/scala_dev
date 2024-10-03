import io.netty.handler.codec.dns.DnsQuestion
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.zookeeper.Transaction


object assignment3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Assignment3")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    //problem 1

    val employeePerformance = List(
      ("E001", "Sales", 85, "2024-02-10", "Sales Manager"),
      ("E002", "HR", 78, "2024-03-15", "HR Assistant"),
      ("E003", "IT", 92, "2024-01-22", "IT Manager"),
      ("E004", "Sales", 88, "2024-02-18", "Sales Rep"),
      ("E005", "HR", 95, "2024-03-20", "HR Manager"),
      ("E006", "IT", 82, "2024-01-30", "IT Consultant"),
      ("E007", "Sales", 90, "2024-02-25", "Sales Manager"),
      ("E008", "HR", 85, "2024-03-25", "HR Manager")
    ).toDF("employee_id", "department", "performance_score", "review_date", "position")

//    val df1 = employeePerformance
//    .withColumn("month",month (col("review_date")))
//      .filter(col("position").like("%Manager") && col("performance_score")>80)



//    val df2 = employeePerformance
//      .groupBy (col("department"),month(col("review_date")).alias("review_month"))
//      .agg(
//        avg("performance_score")
//      ).show()

//    val df3 = employeePerformance.filter(col("performance_score")>90)
//      .groupBy (col("department"),month(col("review_date")).alias("review_month"))
//      .agg(
//        count("employee_id")
//      ).show()



    val customerChurn = List(
      ("C001", "Premium Gold", "Yes", "2023-12-01", 1200, "USA"),
      ("C002", "Basic", "No", null, 400, "Canada"),
      ("C003", "Premium Silver", "Yes", "2023-11-15", 800, "UK"),
      ("C004", "Premium Gold", "Yes", "2024-01-10", 1500, "USA"),
      ("C005", "Basic", "No", null, 300, "India"),
      ("C006", "Premium Silver", "Yes", "2024-01-20", 1300, "UK"),
      ("C007", "Premium Gold", "Yes", "2024-02-15", 1800, "USA"),
      ("C008", "Basic", "No", null, 600, "India"),
      ("C009", "Premium Gold", "Yes", "2023-12-25", 1700, "USA"),
      ("C010", "Premium Silver", "Yes", "2023-11-01", 900, "UK")
    ).toDF("customer_id" ,"subscription_type", "churn_status", "churn_date", "revenue country")











  }

}
