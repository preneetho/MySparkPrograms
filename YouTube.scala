import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object YouTube{
        def main(args: Array[String]): Unit = {
          // Read the CSV file
          val conf = new SparkConf().setAppName("MyTube")
          val sc = new SparkContext(conf)
          val csv =sc.textFile("hdfs:/preneeth_hdfs/youtubedataNew.txt")
          // split / clean data
          val headerAndRows = csv.map(line => line.split("\t")(3))
          
          val countRDD = sc.parallelize(headerAndRows.take(50).map(x => (x,1))).reduceByKey(_ + _)
          
          val sortRDD = countRDD.map (x => x.swap).sortByKey(false)
                    
          sortRDD.foreach(println)
          sc.stop
        }
}