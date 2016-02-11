import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

//Get maximum number of loan given to which grade users (A-G).

object Loan2{

	//Program to fetch records with Grade == E
	
        def removeQuotes(str: String) : String = {
        val newString = str.replaceAll("\"","") 
	        return newString
        }
        
        
        def main(args: Array[String]): Unit = {
          
            val conf = new SparkConf().setAppName("MyLoan2")
            val sc = new SparkContext(conf)
            // Read the CSV file
            val csv =sc.textFile("hdfs:/preneeth_hdfs/LoanData.csv")
            // split / clean data
	    val data = csv.map(line => line.split(",").map(_.trim).map(x => removeQuotes(x)))
	    //eliminate junk data
            val goodData = data.collect { case l if (l.length > 8) => (l)}
	    val mytuple = goodData.map (x => (x(8), x(2).toInt))
	    val result = mytuple.reduceByKey((x,y) => math.max(x,y))
	    result.saveAsTextFile("hdfs:/preneeth_hdfs/result.txt")
	    result.saveAsTextFile("/home/edureka/preneeth/result.txt")
	    
	    result.foreach(println)
        }
}

