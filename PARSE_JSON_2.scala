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

// To select inner JSON from row
for(a<-input_df){println(Try(a.getAs[String]("op_ts")).isSuccess)} //True
for(a<-input_df){println(Try(a.getAs[Row]("after")).isSuccess)} //True
for(a<-input_df){println(Try(a.getAs[Row]("after").getAs[Long]("METRIC_COLUMN_ID")).isSuccess)} //False

//Filtering inner JSON values
input_df.filter($"after.METRIC_GROUP_ID"===4700).show()

// Convert dataframe into map object (for iteration) 
val iterable=input_df.collect.map(r => Map(input_df.columns.zip(r.toSeq):_*))