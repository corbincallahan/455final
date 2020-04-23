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

    val testData = spark.read.textFile("hdfs://indianapolis:13198/cs455/final/sitc.tab").rdd.filter(x =>
      x.split("\t")(1) == "14")
    testData.saveAsTextFile("hdfs://indianapolis:13198/cs455/final/test")
  }

}