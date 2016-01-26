import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object YouTube{
        def main(args: Array[String]): Unit = {
          
          val conf = new SparkConf().setAppName("MyTube")
          val sc = new SparkContext(conf)
          
          // Read the CSV file
          val csv =sc.textFile("hdfs:/preneeth_hdfs/youtubedataNew.txt")
          
          //split each line item into a map
          val data = csv.map(line => line.split("\t").map(e=>e.trim))
          
          //COL4 is not present in few records, so eliminate it and form a tuple with (category, 1)
          val goodData = data.collect { case l if (l.length > 3) => (l(3), 1)}
          
          //sum all categories
          val countRDD = goodData.reduceByKey(_ + _)
          
          //Swap the tuple to sort on key.
          val sortRDD = countRDD.map (x => x.swap).sortByKey(false)
          
          //Sort (desending) and get top 5
          val top5 = sortRDD.sortBy(_._1, false).take(5)
          
          //Print
          top5.foreach(println)
          sc.stop
        }
}
