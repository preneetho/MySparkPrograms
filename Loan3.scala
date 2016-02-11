import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._

//Highest loan amount given in that year with that Employee id and Employees annual income.

object Loan3{

	//Highest loan amount given in that year with that Employee id and Employees annual income.
	
        def removeQuotes(str: String) : String = {
        val newString = str.replaceAll("\"","") 
	        return newString
        }
        
       def getYear(str: String) : Int = {
		val index = str.indexOf("-")
		val newString = str.substring(index+1) 
		try {
			return newString.toInt
		} catch {
		  case ex :  NumberFormatException => {
		  	return 0
		  }
		}
        }
        
        private def deleteFile(path: String) = {
	    val fileTemp = new File(path)
	    if (fileTemp.exists) {
	       fileTemp.delete()
	    }
  	}
 
        def main(args: Array[String]): Unit = {
          
            val conf = new SparkConf().setAppName("MyLoan3")
            val sc = new SparkContext(conf)
            // Read the CSV file
            val csv =sc.textFile("hdfs:/preneeth_hdfs/LoanData.csv")
            // split / clean data
	    val data = csv.map(line => line.split(",").map(_.trim).map(x => removeQuotes(x)))
	    //eliminate junk data
            val goodData = data.collect { case l if (l.length > 11) => (l)}
	    //val mytuple = goodData.map (x => (x(1), x(10), x(2), x(13).toInt, getYear(x(15)).toInt))
	    val yearAmt = goodData.map(x => (getYear(x(15)), x(2).toInt))
	    val result = yearAmt.reduceByKey((x,y) => math.max(x,y))
	    deleteFile("hdfs:/preneeth_hdfs/result.txt")
	    result.saveAsTextFile("hdfs:/preneeth_hdfs/result1.txt")
	    result.foreach(println)
        }
}