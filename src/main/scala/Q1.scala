
import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File
import org.apache.spark.sql.Row
import scala.collection.mutable.ListBuffer
import org.apache.spark.util.IntParam
import org.apache.spark.sql.functions._

object Q1 {
  

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Q1").getOrCreate()
    val sc = SparkContext.getOrCreate()
    import spark.implicits._


    val schema = StructType(Array(StructField("location_id", IntegerType, true), StructField("partner_id", IntegerType, true),
    StructField("product_id", IntegerType, true), StructField("year", IntegerType, true), StructField("export_value", IntegerType, true),
    StructField("import_value", IntegerType, true), StructField("sitc_eci", IntegerType, true), StructField("sitc_coi", IntegerType, true),
    StructField("location_code", StringType, true), StructField("partner_code", StringType, true), StructField("sitc_product_code", StringType, true)))


    val trade = spark.read.option("header", "false").option("delimiter", "\t").schema(schema).csv("/TradeData/country_partner_sitcproduct2digit_year.tab")

    val G8 = trade.select("location_id", "partner_id", "product_id", "year", "export_value", "import_value", "location_code", "partner_code", "sitc_product_code").filter("location_code == 'ITA' or location_code == 'USA' or location_code == 'RUS' or location_code == 'DEU' or location_code == 'GBR' or location_code == 'JPN' or location_code == 'FRA' or location_code == 'CAN'")

    val G8onlyG8 = G8.select("location_id", "partner_id", "product_id", "year", "export_value", "import_value", "location_code", "partner_code", "sitc_product_code").filter("partner_code == 'ITA' or partner_code == 'USA' or partner_code == 'RUS' or partner_code == 'DEU' or partner_code == 'GBR' or partner_code == 'JPN' or partner_code == 'FRA' or partner_code == 'CAN'")

    val G8TopExports = G8onlyG8.sort(desc("export_value")).limit(5)

    val writer = G8TopExports.write
    writer.format("csv").save("/TP-Q1/Q1.csv")
    writer.save
  }


}