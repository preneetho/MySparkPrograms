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
          //val data = csv.map(line => line.split("\t")(3)).map(e=>e.trim)
          
          val data = csv.map(line => line.split("\t").map(e=>e.trim))
          
          val goodData = data.collect { case l if (l.length > 3) => (l(3), 1)}
          
          val countRDD = goodData.reduceByKey(_ + _)
          
          val sortRDD = countRDD.map (x => x.swap).sortByKey(false)
          
          val top5 = sortRDD.sortBy(_._1, false).take(5)
                    
          sortRDD.foreach(println)
          sc.stop
          //Modified files on Server
        }
}
