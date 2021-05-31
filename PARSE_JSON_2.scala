import org.apache.spark.sql.SparkSession
import spark.implicits._ // For implicit conversions like converting RDDs to DataFrames


// In the Spark shell you can wrap your multiple line Spark code in parenthesis to execute the code. 

val spark = SparkSession(
	.builder()
	.appName("Spark SQL basic example")
	.config("spark.some.config.option", "some-value")
	.getOrCreate()
)

// Read json file into dataframe
val input_df = spark.read.option("multiline",true).json("input_format.txt")
input_df.printSchema()
input_df.show()

// To select inner JSON (not a list-- you need explode for that)
input_df.select("after.*").show()





