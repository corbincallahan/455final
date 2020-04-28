import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object TestAnalysis {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("ExampleAnalysis").getOrCreate()
    val sc = SparkContext.getOrCreate()
    import spark.implicits._

    val data = sc.textFile("hdfs://jackson:13198/cs455/final/sitc.tab")
	val header = data.first()
	val dataNoHeader = data.filter(line => line != header)

	val beforeCrash = dataNoHeader.filter(line => line.split("\t")(3).toInt > 1995 && line.split("\t")(3).toInt < 2000)
	val beforeCrashSums = beforeCrash.map(line => (line.split("\t")(10), line.split("\t")(4).toLong)).reduceByKey(_ + _)

	val afterCrash = dataNoHeader.filter(line => line.split("\t")(3).toInt > 2001 && line.split("\t")(3).toInt < 2006)
	val afterCrashSums = afterCrash.map(line => (line.split("\t")(10), line.split("\t")(4).toLong)).reduceByKey(_ + _)

	beforeCrashSums.saveAsTextFile("hdfs://jackson:13198/cs455/final/beforeCrashSums")
	afterCrashSums.saveAsTextFile("hdfs://jackson:13198/cs455/final/afterCrashSums")
  }

}