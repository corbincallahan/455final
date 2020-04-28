import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Q6 {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Q6").getOrCreate()
    val sc = SparkContext.getOrCreate()
    import spark.implicits._

    val data = sc.textFile("hdfs://jackson:13198/cs455/final/sitc.tab")
	val header = data.first()
	val dataNoHeader = data.filter(line => line != header)

	val beforeRec = dataNoHeader.filter(line => line.split("\t")(3).toInt > 2003 && line.split("\t")(3).toInt < 2008)
	val beforeRecSums = beforeRec.map(line => (line.split("\t")(10), line.split("\t")(4).toLong)).reduceByKey(_ + _)

	val afterRec = dataNoHeader.filter(line => line.split("\t")(3).toInt > 2007 && line.split("\t")(3).toInt < 2012)
	val afterRecSums = afterRec.map(line => (line.split("\t")(10), line.split("\t")(4).toLong)).reduceByKey(_ + _)

	beforeRecSums.saveAsTextFile("hdfs://jackson:13198/cs455/final/beforeRecSums")
	afterRecSums.saveAsTextFile("hdfs://jackson:13198/cs455/final/afterRecSums")
  }

}