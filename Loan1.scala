import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Loan1{
        def main(args: Array[String]): Unit = {
          
            val conf = new SparkConf().setAppName("MyLoan1")
            val sc = new SparkContext(conf)
          
            // Read the CSV file
            val csv =sc.textFile("hdfs:/preneeth_hdfs/LoanData.csv")
          
            // split / clean data
	    val data = csv.map(line => line.split(",").map(_.trim).map(x => removeQuotes(x)))
	   
	   //eliminate junk data
          val goodData = data.collect { case l if (l.length > 8) => (l)}
	   
	   //eliminate junk data
           val filterData = goodData.filter(x => x.map(y => y.split(",")(8) == "E"))
           
           filterData.foreach(println)
	   
        }
        
        def removeQuotes(str: String) : String = {
        val newString = str.replaceAll("\"","") 
        return newString
        }
        
        
}