import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._

//Highest loan amount given in that year with that Employee id and Employees annual income.

object Loan4{

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
        
        
 
        def main(args: Array[String]): Unit = {
          
         
          val conf = new SparkConf().setAppName("MyLoan3")
            val sc = new SparkContext(conf)
         val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	 
	 // Create an RDD
	 val people = sc.textFile("hdfs:/preneeth_hdfs/LoanData.csv")
	 
	 // The schema is encoded in a string
	 val schemaString = "COL_0 COL_1 COL_2 COL_3 COL_4 COL_5 COL_6 COL_7 COL_8 COL_9 COL_10 COL_11 COL_12 COL_13 COL_14 COL_15 COL_16 COL_17 COL_18 COL_19 COL_20 COL_21 COL_22 COL_23 COL_24 COL_25 COL_26 COL_27 COL_28 COL_29 COL_30 COL_31 COL_32 COL_33 COL_34 COL_35 COL_36 COL_37 COL_38 COL_39 COL_40 COL_41 COL_42 COL_43 COL_44 COL_45 COL_46 COL_47 COL_48 COL_49 COL_50 COL_51"
	 
	 // Import Row.
	 import org.apache.spark.sql.Row;
	 
	 // Import Spark SQL data types
	 import org.apache.spark.sql.types.{StructType,StructField,StringType};
	 
	 // Generate the schema based on the string of schema
	 val schema =
	   StructType(
	     schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
	 
	 // Convert records of the RDD (people) to Rows.
	 val rowRDD = people.map(_.split(",")).map(p => Row(removeQuotes(p(0)), removeQuotes( p(1)).trim, removeQuotes( p(2)), removeQuotes( p(3)), removeQuotes( p(4)), removeQuotes( p(5)), removeQuotes( p(6)), removeQuotes( p(7)), removeQuotes( p(8)) , removeQuotes( p(9)), removeQuotes( p(10)), removeQuotes( p(11)).trim, removeQuotes( p(12)), removeQuotes( p(13)), removeQuotes( p(14)), getYear(removeQuotes( p(15))).toString, removeQuotes( p(16)), removeQuotes( p(17)), removeQuotes( p(18)) , removeQuotes( p(19)), removeQuotes( p(20)), removeQuotes( p(21)).trim, removeQuotes( p(22)), removeQuotes( p(23)), removeQuotes( p(24)), removeQuotes( p(25)), removeQuotes( p(26)), removeQuotes( p(27)), removeQuotes( p(28)) , removeQuotes( p(29)), removeQuotes( p(30)), removeQuotes( p(31)).trim, removeQuotes( p(32)), removeQuotes( p(33)), removeQuotes( p(34)), removeQuotes( p(35)), removeQuotes( p(36)), removeQuotes( p(37)), removeQuotes( p(38)) , removeQuotes( p(39)), removeQuotes( p(40)), removeQuotes( p(41)).trim, removeQuotes( p(42)), removeQuotes( p(43)), removeQuotes( p(44)), removeQuotes( p(45)), removeQuotes( p(46)), removeQuotes( p(47)), removeQuotes( p(48)) , removeQuotes( p(49)), removeQuotes( p(50)), removeQuotes( p(51))))
	 
	 // Apply the schema to the RDD.
	 val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
	 
	 // Register the DataFrames as a table.
	 peopleDataFrame.registerTempTable("loan")
	 
	 // SQL statements can be run by using the sql methods provided by sqlContext.
	 val results = sqlContext.sql("SELECT COL_2, COL_15, COL_10 FROM loan")

            //results.saveAsTextFile("hdfs:/preneeth_hdfs/result100.txt")
	    results.foreach(println)
        }
}