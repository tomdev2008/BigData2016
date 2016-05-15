import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Date

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Vitaliy on 5/14/2016.
 */
object SparkMain2 {

  def main(args: Array[String]) {
    println("Start")

    val conf = new SparkConf()
      .setAppName("HW1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val keywordsSchema = StructType(Array(
      StructField("tagId", StringType, true),
      StructField("tags", StringType, true),
      StructField("f1", StringType, true),
      StructField("f2", StringType, true),
      StructField("f3", StringType, true),
      StructField("url", StringType, true)))

        val users = sqlContext.read
          .schema(keywordsSchema)
          .format("com.databricks.spark.csv")
          .option("delimiter", "\t")
          .load("D:\\projects\\BDCC\\BigData2016\\spark1hw4\\tags.txt")
        users.registerTempTable("tags")
        users.printSchema

    sqlContext.sql("select * from tags").foreach(println)

//    val users = sqlContext.read
//      .format("com.databricks.spark.csv")
//      .option("delimiter", "\t")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .load("D:\\projects\\BDCC\\BigData2016\\spark1hw4\\users.csv")
//    users.registerTempTable("users")
//    users.printSchema
//
//    val roles = sqlContext.read
//      .format("com.databricks.spark.csv")
//      .option("delimiter", "\t")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .load("D:\\projects\\BDCC\\BigData2016\\spark1hw4\\roles.csv")
//    roles.registerTempTable("roles")
//    roles.printSchema
//
//    sqlContext.sql("select u.name, u.roleId from users u").foreach(println)
//    sqlContext.sql("select r.role from roles r").foreach(println)
//    sqlContext.sql(
//      "select u.name, r.role from users u join roles r on u.roleId = r.id"
//    ).foreach(println)

  }



}



