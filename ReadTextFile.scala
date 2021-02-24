import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ReadTextFile {

  def main(args : Array[String]) : Unit = {

    var conf = new SparkConf().setAppName("Read Text File in Spark").setMaster("local[*]")
    //val sc = new SparkContext(conf)
    val textRDD = sc.textFile(args(0))
    // Read RDD
    textRDD.collect().foreach(println)
    // Get Header of the File
    val header = textRDD.first()
    // Remove header
    val filterRDD = textRDD.filter(row => row != header)
    // Read RDD
    filterRDD.collect().foreach(println)
    // Data Count
    filterRDD.count
  }
}



