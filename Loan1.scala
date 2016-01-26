import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Loan1{

	//Program to fetch records with Grade == E
	
        def removeQuotes(str: String) : String = {
	        val newString = str.replaceAll("\"","") 
	        return newString
        }
        
        
        def main(args: Array[String]): Unit = {
          
            val conf = new SparkConf().setAppName("MyLoan1")
            val sc = new SparkContext(conf)
            // Read the CSV file
            val csv =sc.textFile("hdfs:/preneeth_hdfs/LoanData.csv")
            // split / clean data
	    val data = csv.map(line => line.split(",").map(_.trim).map(x => removeQuotes(x)))
	    //eliminate junk data
            val goodData = data.collect { case l if (l.length > 8) => (l)}
	    val header = List ("COL1","COL2","COL3","COL4","COL5","COL6","COL7","COL8","COL9")
	    val maps = goodData.map(splits => header.zip(splits).toMap)
            val result = maps.filter(x => x("COL9") == "E")
            result.foreach(println)
        }
}