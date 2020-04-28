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

object Q4 {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Q4").getOrCreate()
    val sc = SparkContext.getOrCreate()
    import spark.implicits._


    val schema = StructType(Array(StructField("location_id", IntegerType, true), StructField("partner_id", IntegerType, true),
    StructField("product_id", IntegerType, true), StructField("year", IntegerType, true), StructField("export_value", IntegerType, true),
    StructField("import_value", IntegerType, true), StructField("sitc_eci", IntegerType, true), StructField("sitc_coi", IntegerType, true),
    StructField("location_code", StringType, true), StructField("partner_code", StringType, true), StructField("sitc_product_code", IntegerType, true)))

    val trade = spark.read.option("header", "false").option("delimiter", "\t").schema(schema).csv("/TradeData/country_partner_sitcproduct2digit_year.tab")

    // 1965 Neutral and 1966 El Nino
    val get65 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1965")     
    val get66 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1966")  
    val sum65 = get65.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum66 = get66.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum65rename = sum65.withColumnRenamed("sum(export_value)", "export_65") 
    val sum66rename = sum66.withColumnRenamed("sum(export_value)", "export_66") 
    val join65and66 = sum65rename.join(sum66rename, "sitc_product_code").limit(70)

    join65and66.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Cycle65_66.csv")

    val sum65all = join65and66.agg(sum("export_65"))
    val sum66all = join65and66.agg(sum("export_66"))

    sum65all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total65.csv")
    sum66all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total66.csv")

    // 1972-73 Cycle
    val get72 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1972")     
    val get73 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1973")  
    val sum72 = get72.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum73 = get73.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum72rename = sum72.withColumnRenamed("sum(export_value)", "export_72") 
    val sum73rename = sum73.withColumnRenamed("sum(export_value)", "export_73") 
    val join72and73 = sum72rename.join(sum73rename, "sitc_product_code").limit(70)

    join72and73.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Cycle72_73.csv")

    val sum72all = join72and73.agg(sum("export_72"))
    val sum73all = join72and73.agg(sum("export_73"))

    sum72all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total72.csv")
    sum73all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total73.csv")

    // 1977-78 Cycle
    val get77 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1977")     
    val get78 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1978")  
    val sum77 = get77.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum78 = get78.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum77rename = sum77.withColumnRenamed("sum(export_value)", "export_77") 
    val sum78rename = sum66.withColumnRenamed("sum(export_value)", "export_78") 
    val join77and78 = sum77rename.join(sum78rename, "sitc_product_code").limit(70)

    join77and78.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Cycle77_78.csv")

    val sum77all = join77and78.agg(sum("export_77"))
    val sum78all = join77and78.agg(sum("export_78"))

    sum77all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total77.csv")
    sum78all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total78.csv")

    // 1979-80 Cycle
    val get79 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1979")     
    val get80 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1980")  
    val sum79 = get79.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum80 = get80.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum79rename = sum79.withColumnRenamed("sum(export_value)", "export_79") 
    val sum80rename = sum66.withColumnRenamed("sum(export_value)", "export_80") 
    val join79and80 = sum79rename.join(sum80rename, "sitc_product_code").limit(70)

    join79and80.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Cycle79_80.csv")

    val sum79all = join79and80.agg(sum("export_79"))
    val sum80all = join79and80.agg(sum("export_80"))

    sum79all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total79.csv")
    sum80all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total80.csv")

    // 1982-83 Cycle
    val get82 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1982")     
    val get83 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1983")  
    val sum82 = get82.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum83 = get83.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum82rename = sum82.withColumnRenamed("sum(export_value)", "export_82") 
    val sum83rename = sum83.withColumnRenamed("sum(export_value)", "export_83") 
    val join82and83 = sum82rename.join(sum83rename, "sitc_product_code").limit(70)

    join82and83.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Cycle82_83.csv")

    val sum82all = join82and83.agg(sum("export_82"))
    val sum83all = join82and83.agg(sum("export_83"))

    sum82all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total82.csv")
    sum83all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total83.csv")

    // 1986-87 Cycle
    val get86 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1986")     
    val get87 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1987")  
    val sum86 = get86.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum87 = get87.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum86rename = sum86.withColumnRenamed("sum(export_value)", "export_86") 
    val sum87rename = sum87.withColumnRenamed("sum(export_value)", "export_87") 
    val join86and87 = sum86rename.join(sum87rename, "sitc_product_code").limit(70)

    join86and87.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Cycle86_87.csv")

    val sum86all = join86and87.agg(sum("export_86"))
    val sum87all = join86and87.agg(sum("export_87"))

    sum86all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total86.csv")
    sum87all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total87.csv")

    // 1991-92 Cycle
    val get91 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1991")     
    val get92 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1992")  
    val sum91 = get91.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum92 = get92.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum91rename = sum91.withColumnRenamed("sum(export_value)", "export_91") 
    val sum92rename = sum92.withColumnRenamed("sum(export_value)", "export_92") 
    val join91and92 = sum91rename.join(sum92rename, "sitc_product_code").limit(70)

    join91and92.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Cycle91_92.csv")

    val sum91all = join91and92.agg(sum("export_91"))
    val sum92all = join91and92.agg(sum("export_92"))

    sum91all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total91.csv")
    sum92all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total92.csv")

    // 1994-95 Cycle
    val get94 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1994")     
    val get95 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1995")  
    val sum94 = get94.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum95 = get95.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum94rename = sum94.withColumnRenamed("sum(export_value)", "export_94") 
    val sum95rename = sum95.withColumnRenamed("sum(export_value)", "export_95") 
    val join94and95 = sum94rename.join(sum95rename, "sitc_product_code").limit(70)

    join94and95.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Cycle94_95.csv")

    val sum94all = join94and95.agg(sum("export_94"))
    val sum95all = join94and95.agg(sum("export_95"))

    sum94all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total94.csv")
    sum95all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total95.csv")

    // 1997-98 Cycle
    val get97 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1997")     
    val get98 = trade.select("year", "export_value", "sitc_product_code").filter("year == 1998")  
    val sum97 = get97.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum98 = get98.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum97rename = sum97.withColumnRenamed("sum(export_value)", "export_97") 
    val sum98rename = sum98.withColumnRenamed("sum(export_value)", "export_98") 
    val join97and98 = sum97rename.join(sum98rename, "sitc_product_code").limit(70)

    join97and98.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Cycle97_98.csv")

    val sum97all = join97and98.agg(sum("export_97"))
    val sum98all = join97and98.agg(sum("export_98"))

    sum97all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total97.csv")
    sum98all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total98.csv")

    // 2002-03 Cycle
    val get02 = trade.select("year", "export_value", "sitc_product_code").filter("year == 2002")     
    val get03 = trade.select("year", "export_value", "sitc_product_code").filter("year == 2003")  
    val sum02 = get02.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum03 = get03.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum02rename = sum02.withColumnRenamed("sum(export_value)", "export_02") 
    val sum03rename = sum03.withColumnRenamed("sum(export_value)", "export_03") 
    val join02and03 = sum02rename.join(sum03rename, "sitc_product_code").limit(70)

    join02and03.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Cycle02_03.csv")

    val sum02all = join02and03.agg(sum("export_02"))
    val sum03all = join02and03.agg(sum("export_03"))

    sum02all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total02.csv")
    sum03all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total03.csv")

    // 2006-07 Cycle
    val get06 = trade.select("year", "export_value", "sitc_product_code").filter("year == 2006")     
    val get07 = trade.select("year", "export_value", "sitc_product_code").filter("year == 2007")  
    val sum06 = get06.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum07 = get07.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum06rename = sum06.withColumnRenamed("sum(export_value)", "export_06") 
    val sum07rename = sum07.withColumnRenamed("sum(export_value)", "export_07") 
    val join06and07 = sum06rename.join(sum07rename, "sitc_product_code").limit(70)

    join06and07.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Cycle06_07.csv")

    val sum06all = join06and07.agg(sum("export_06"))
    val sum07all = join06and07.agg(sum("export_07"))

    sum06all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total06.csv")
    sum07all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total07.csv")

    // Cycle 2009-10
    val get09 = trade.select("year", "export_value", "sitc_product_code").filter("year == 2009")     
    val get10 = trade.select("year", "export_value", "sitc_product_code").filter("year == 2010")  
    val sum09 = get09.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum10 = get10.groupBy($"sitc_product_code").agg(sum($"export_value")).sort(col("sitc_product_code").desc)
    val sum09rename = sum09.withColumnRenamed("sum(export_value)", "export_09") 
    val sum10rename = sum10.withColumnRenamed("sum(export_value)", "export_10") 
    val join09and10 = sum09rename.join(sum10rename, "sitc_product_code").limit(70)

    join09and10.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Cycle09_10.csv")

    val sum09all = join09and10.agg(sum("export_09"))
    val sum10all = join09and10.agg(sum("export_10"))

    sum09all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total09.csv")
    sum10all.write.mode("overwrite").format("csv").option("header", "true").save("/TP-Q4/Total10.csv")

  }
}