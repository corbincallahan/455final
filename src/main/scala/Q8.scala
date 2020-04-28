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

object Q8 {
  

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Q8").getOrCreate()
    val sc = SparkContext.getOrCreate()
    import spark.implicits._


    val schema = StructType(Array(StructField("location_id", IntegerType, true), StructField("partner_id", IntegerType, true),
    StructField("product_id", IntegerType, true), StructField("year", IntegerType, true), StructField("export_value", IntegerType, true),
    StructField("import_value", IntegerType, true), StructField("sitc_eci", IntegerType, true), StructField("sitc_coi", IntegerType, true),
    StructField("location_code", StringType, true), StructField("partner_code", StringType, true), StructField("sitc_product_code", StringType, true)))


    val trade = spark.read.option("header", "false").option("delimiter", "\t").schema(schema).csv("/TradeData/country_partner_sitcproduct2digit_year.tab")

    val mex = trade.select("location_code", "partner_code", "export_value", "year").filter("location_code == 'MEX'")

    val mex_usa_can = mex.filter("partner_code == 'USA' or partner_code == 'CAN'")

    val mex_naftayears = mex_usa_can.filter("year == 1992 or year == 1993 or year == 1994 or year == 1995 or year == 1996 or year == 1997 or year == 1998")
    val mexTotalAnnualExports = mex_naftayears.groupBy("year", "location_code").agg( sum("export_value")).sort(col("year").desc).limit(7)

    // val writer = mexTotalAnnualExports.write
    // writer.format("csv").option("mode", "APPEND").save("/TP-Q8/Mexico.csv")
    // writer.save

    mexTotalAnnualExports.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q8/Mexico2.csv")

    val can = trade.select("location_code", "partner_code", "export_value", "year").filter("location_code == 'CAN'")
    val can_usa_mex = can.filter("partner_code == 'USA' or partner_code == 'MEX'")
    val can_naftayears = can_usa_mex.filter("year == 1992 or year == 1993 or year == 1994 or year == 1995 or year == 1996 or year == 1997 or year == 1998")
    val canTotalAnnualExports = can_naftayears.groupBy("year", "location_code").agg( sum("export_value")).sort(col("year").desc).limit(7)

    canTotalAnnualExports.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q8/Canada.csv")

    val usa = trade.select("location_code", "partner_code", "export_value", "year").filter("location_code == 'USA'")
    val usa_can_mex = usa.filter("partner_code == 'CAN' or partner_code == 'MEX'")
    val usa_naftayears = usa_can_mex.filter("year == 1992 or year == 1993 or year == 1994 or year == 1995 or year == 1996 or year == 1997 or year == 1998")
    val usaTotalAnnualExports = can_naftayears.groupBy("year", "location_code").agg( sum("export_value")).sort(col("year").desc).limit(7)

    canTotalAnnualExports.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q8/UnitedStates.csv")


  
  }

}
