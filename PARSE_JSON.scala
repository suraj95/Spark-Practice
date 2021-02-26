import org.apache.spark.sql.SparkSession
import spark.implicits._ // For implicit conversions like converting RDDs to DataFrames


// In the Spark shell you can wrap your multiple line Spark code in parenthesis to execute the code. 

val spark = SparkSession(
	.builder()
	.appName("Spark SQL basic example")
	.config("spark.some.config.option", "some-value")
	.getOrCreate()
)

// Read json file into IP dataframe
val ip_df = spark.read.option("multiline",true).json("FEDirector_port_data.txt")
ip_df.printSchema()
ip_df.show()


// Select storageid
ip_df.withColumn("storageid-flat",explode($"storageidlist.storageid")).show()


// Select directorid
ip_df.withColumn("storageid-flat",explode($"storageidlist.storageid")).withColumn("directorid-list", explode($"storageidlist.fedirectorList")).withColumn("directorId-flat", explode($"directorid-list.directorId")).show()


// Select portMetricDataList
ip_df.withColumn("storageid-flat",explode($"storageidlist.storageid")).withColumn("directorid-list", explode($"storageidlist.fedirectorList")).withColumn("directorId-flat", explode($"directorid-list.directorId")).withColumn("portMetricDataList", explode($"directorid-list.portMetricDataList")).show()


// Drop useless columns

val cleaned_df = ip_df.withColumn("storageid-flat",explode($"storageidlist.storageid")).withColumn("directorid-list", explode($"storageidlist.fedirectorList")).withColumn("directorId-flat", explode($"directorid-list.directorId")).withColumn("portMetricDataList", explode($"directorid-list.portMetricDataList")).drop("errorcode").drop("errormessage").drop("label").drop("status").drop("sublabel").drop("ts").drop("storageidlist").drop("directorid-list")
cleaned_df.show()


// Split portMetricDataList portMetricData-1 into portMetricData-2

cleaned_df.withColumn("portMetricData-1", explode($"portMetricDataList".getItem(0))).withColumn("portMetricData-2", explode($"portMetricDataList".getItem(1)))show()

val metric_df = cleaned_df.withColumn("portMetricData-1", explode($"portMetricDataList".getItem(0))).withColumn("portMetricData-2", explode($"portMetricDataList".getItem(1)))


// Select metricid 

val metricid_df= metric_df.withColumn("metricid-1",$"portMetricData-1.metricid").withColumn("metricid-2",$"portMetricData-2.metricid")
metricid_df.show()


// Select value and ts

val data_df= metricid_df.withColumn("data-1-1",$"portMetricData-1.data".getItem(0)).withColumn("data-1-2",$"portMetricData-1.data".getItem(1)).withColumn("data-2-1",$"portMetricData-1.data".getItem(0)).withColumn("data-2-2",$"portMetricData-2.data".getItem(1))
data_df.show()
val final_df = data_df.drop("portMetricData-1","portMetricData-2")

val answer_df= final_df.withColumn("value-1-1",$"data-1-1.value").withColumn("value-1-2",$"data-1-2.value").withColumn("ts-1-1",$"data-1-1.ts").withColumn("ts-1-2",$"data-1-2.ts")

val FINAL = answer_df.drop("portMetricDataList", "data-1-1", "data-1-2", "data-2-1", "data-2-2")

// Write dataframe to csv

FINAL.write.csv("Assignment.csv")



