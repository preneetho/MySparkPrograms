import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object YouTube2{
        def main(args: Array[String]): Unit = {
          
          val conf = new SparkConf().setAppName("MyTube")
          val sc = new SparkContext(conf)
          // Read the CSV file
          val csv =sc.textFile("hdfs:/preneeth_hdfs/youtubedataNew.txt")
          
          //split the line items
          val data = csv.map(line => line.split("\t").map(e=>e.trim))
          
          //eliminate junk data
          val goodData = data.collect { case l if (l.length > 6) => (l(6), l(1))}
          
          //sort and get top 10
          val top10 = goodData.sortBy(_._1, false).take(10)
                    
          top10.foreach(println)

          sc.stop
        }
}