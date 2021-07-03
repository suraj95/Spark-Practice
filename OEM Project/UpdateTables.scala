package com.jio.oem

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{ SparkConf, SparkContext }
import com.jio.utilities._
import com.jio.utilities.ReadInput._
import com.jio.utilities.StoreOutput._
import java.util.Properties
import java.sql._

import org.apache.spark.sql.SaveMode

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedOutputStream,BufferedInputStream}
import java.net.URI

import collection.mutable.Map


object UpdateTables {
  

   def updateOracleDB(spark: SparkSession, retrieved_table_df: DataFrame, records_df: DataFrame,connectnPro: Properties, tableName: String, PrimaryKey : String, Ot_type: String, Column_String: String, Datatype_String: String) {

     import spark.implicits._
          
     var resultDF = spark.emptyDataFrame
     
     resultDF = processUpdates(spark,retrieved_table_df,records_df,Ot_type,PrimaryKey, Column_String,Datatype_String)             
     resultDF.createOrReplaceTempView("FinalTable")
     spark.table("FinalTable").count //force action
     
     // Don't remove! To prevent Table from getting dropped when overwriting 
     if(resultDF!=spark.emptyDataFrame){
       
       val jdbcUrl = connectnPro.getProperty("jdbcUrl")
       val userid = connectnPro.getProperty("user")
       val password = connectnPro.getProperty("password")
       
       spark.table("FinalTable").write.mode(SaveMode.Overwrite).option("truncate", true).jdbc(url = jdbcUrl, table = tableName,connectnPro) //replace("SYSMAN.","")
     }
                   
 }   
     
   
   def processUpdates(spark: SparkSession, retrieved_table_df: DataFrame, records_df: DataFrame, Ot_type: String, PrimaryKey: String, Column_String: String, Datatype_String: String): DataFrame ={
    
     import spark.implicits._
     
     var resultDF = spark.emptyDataFrame
     var CompositeDF = spark.emptyDataFrame
     val Composite_key = PrimaryKey.split(',')
     val Composite_Seq = Composite_key.toSeq
     
     println(Ot_type)
     println(Composite_Seq)
     
     
     var mapped_columns = retrieved_table_df.columns.filter(!_.contains("INSERT_DATE")).filter(!_.contains("UPDATE_DATE"))
     
     var filtered_records = records_df.filter($"op_type"===Ot_type)
     
     if(Ot_type=="I"){            
       CompositeDF = filtered_records.select("after.*").select(mapped_columns.map(col):_*)
       CompositeDF = CastColumn(spark,CompositeDF,Column_String,Datatype_String)
       CompositeDF = CompositeDF.withColumn("INSERT_DATE", current_timestamp())
                                .withColumn("UPDATE_DATE",current_timestamp())
                                
       resultDF = retrieved_table_df.union(CompositeDF)
       //println("ResultDF")
       //resultDF.show()
     }
     
     else if(Ot_type=="U"){
       
       
       CompositeDF = filtered_records.select("before.*").select(mapped_columns.map(col):_*)
       CompositeDF = CastColumn(spark,CompositeDF,Column_String,Datatype_String)
       CompositeDF.printSchema()
       
       val matching_df = retrieved_table_df.join(CompositeDF, Composite_Seq, "inner")
       
       // Check if records exist
       if(matching_df.count==0){
         
       // add new records 
         CompositeDF = filtered_records.select("after.*").select(mapped_columns.map(col):_*)
         CompositeDF = CastColumn(spark,CompositeDF,Column_String,Datatype_String)
         CompositeDF = CompositeDF.withColumn("INSERT_DATE", current_timestamp())
                                .withColumn("UPDATE_DATE",current_timestamp())
         resultDF = retrieved_table_df.union(CompositeDF)
         resultDF= resultDF.dropDuplicates(Composite_Seq)         
       }
       
       else{
         // update existing records and keep the insert_date column
         
         CompositeDF = filtered_records.select("after.*").select(mapped_columns.map(col):_*)
         CompositeDF = CastColumn(spark,CompositeDF,Column_String,Datatype_String)
         CompositeDF = CompositeDF.withColumn("INSERT_DATE", lit(null).cast(TimestampType))
                                .withColumn("UPDATE_DATE",current_timestamp())
         
         val initial_column_names = retrieved_table_df.columns.toSeq
         val renamed_column_names = initial_column_names.map("renamed_" + _)   
         val Composite_Seq_renamed = Composite_Seq.map("renamed_" + _)
         
         val renamed_columns = initial_column_names.map(name=>col(name).as(s"renamed_$name"))
         
         
         val zipped_composite_seq = Composite_Seq zip Composite_Seq_renamed
         val zipped_composite_seq_array = zipped_composite_seq.toArray
         
         
         println(zipped_composite_seq_array)
         
         val df_renamed = retrieved_table_df.select(renamed_columns:_*)
         val join_condition = zipped_composite_seq_array.map(x=>col(x._1)===col(x._2)).reduce(_ && _)
         
         println(join_condition.toString())
         
         var Joined_df = df_renamed.join(CompositeDF,join_condition,"inner") //user inner for common values
         
         // when right values are null, then copy left values into right values
         val umatched_df = df_renamed.join(CompositeDF, join_condition, "left_anti")
         
         // but when right values are not null, we need updates from there
         
         Joined_df = Joined_df.drop("INSERT_DATE").withColumn("new_INSERT_DATE", col("renamed_INSERT_DATE"))       
         Joined_df = Joined_df.drop(renamed_column_names:_*).withColumnRenamed("new_INSERT_DATE","INSERT_DATE")
                 
         Joined_df.printSchema()
         Joined_df.show(false)       
         Joined_df = Joined_df.union(umatched_df)
         
               
         resultDF = Joined_df
       }       

     }
     
     else if(Ot_type=="D"){
       CompositeDF = filtered_records.select("before.*").select(mapped_columns.map(col):_*)
       CompositeDF = CastColumn(spark,CompositeDF,Column_String,Datatype_String)
       CompositeDF = CompositeDF.withColumn("INSERT_DATE", lit(null).cast(TimestampType)) //only needed for join compatability
                                .withColumn("UPDATE_DATE",lit(null).cast(TimestampType))
       
       println("Retrived table",retrieved_table_df.count)
       //retrieved_table_df.printSchema()
       
       //println("CompositeDF", CompositeDF.count)
       //CompositeDF.printSchema()
       
       resultDF = retrieved_table_df.join(CompositeDF, Composite_Seq, "left_anti") // removes records from left that are in right
       println("resultDF",resultDF.count)
       
     }
     
     resultDF= resultDF.dropDuplicates(Composite_Seq)
     
     println("Retrived table",retrieved_table_df.count)
     println("resultDF",resultDF.count) // Do not remove this! Since spark is lazily evaluated, it forces an action
     resultDF
   }
   
   
 def purgeOracleTables(spark: SparkSession,retrieved_table_df: DataFrame, connectnPro: Properties, tableName: String, PrimaryKey: String, Purging_days: BigDecimal,Purging_column: String): DataFrame ={
   
   import spark.implicits._
          
   var resultDF = spark.emptyDataFrame
     
   resultDF = PurgeRecords(spark,retrieved_table_df,tableName,PrimaryKey,Purging_days,Purging_column)  
   resultDF.createOrReplaceTempView("FinalTable")
   spark.table("FinalTable").count //force action
   
   //spark.table("FinalTable").show()
   //println((resultDF!=spark.emptyDataFrame))
   
   // Don't remove! To prevent Table from getting dropped when overwriting
   if(resultDF!=spark.emptyDataFrame){
       
       val jdbcUrl = connectnPro.getProperty("jdbcUrl")
       val userid = connectnPro.getProperty("user")
       val password = connectnPro.getProperty("password")
       
       spark.table("FinalTable").write.mode(SaveMode.Overwrite).jdbc(url = jdbcUrl, table = tableName,connectnPro) //replace("SYSMAN.","")
    }
   
    resultDF    
  }
    
   def PurgeRecords(spark: SparkSession,retrieved_table_df: DataFrame, tableName: String, PrimaryKey: String, Purging_days: BigDecimal,Purging_column: String): DataFrame ={
       
     import spark.implicits._
     
     val Composite_key = PrimaryKey.split(',')
     val Composite_Seq = Composite_key.toSeq
     var purged_records = spark.emptyDataFrame
     var resultDF = spark.emptyDataFrame
     var filter_column: String = null
        
     //println(Purging_column)
     //println(Purging_column==null)
     
     // Purge by Update date
     if(Purging_column==null){       
       filter_column = "UPDATE_DATE"   
      }
     else{
       filter_column = Purging_column
     }
     
     retrieved_table_df.count
     
     retrieved_table_df.show(false)
     
     purged_records = retrieved_table_df.withColumn("Purging_timestamp",col(filter_column).cast("timestamp") + expr("INTERVAL "+Purging_days+" DAYS" ))
                                        .withColumn("Current_timestamp", current_timestamp()) 
                                        
     purged_records.show(false)
     purged_records= purged_records.filter(col("Purging_timestamp")<=col("Current_timestamp")) // check for records whose purging date has exceeded
     purged_records = purged_records.drop("Purging_timestamp","Current_timestamp")
        
     resultDF = retrieved_table_df.join(purged_records, Composite_Seq, "left_anti") //remove purged records
     resultDF = resultDF.dropDuplicates(Composite_Seq)    
     resultDF.show(false)
     
     resultDF.count
    
     resultDF
            
   }
     
   def updateHDFSFiles(spark: SparkSession, retrieved_table_df: DataFrame, records_df: DataFrame, HDFS_Connection: FileSystem, HDFS_Location: String, tableName: String, PrimaryKey : String, Ot_type: String, Column_String: String, Datatype_String: String){
     
     import spark.implicits._
          
     var filtered_records = records_df.filter($"op_type"===Ot_type)
     var resultDF = spark.emptyDataFrame
     
     resultDF = processUpdates(spark,retrieved_table_df,records_df,Ot_type,PrimaryKey, Column_String,Datatype_String)    
     resultDF.createOrReplaceTempView("FinalTable")
     
     // Don't remove! To prevent Table from getting dropped when overwriting 
     if(resultDF!=spark.emptyDataFrame){  
       resultDF.write.format("orc").mode(SaveMode.Overwrite).option("truncate", true).save(HDFS_Connection.getUri().toString()+HDFS_Location+"/"+tableName)       
     }
         
   }
   
   def purgeHDFSFiles(spark: SparkSession, retrieved_table_df: DataFrame, HDFS_Connection: FileSystem, HDFS_Location: String, tableName: String, PrimaryKey: String, Purging_days: BigDecimal, Purging_column: String): DataFrame ={
          
     import spark.implicits._
        
     var resultDF = spark.emptyDataFrame
   
     resultDF = PurgeRecords(spark,retrieved_table_df,tableName,PrimaryKey,Purging_days,Purging_column)
     resultDF.createOrReplaceTempView("FinalTable")
     spark.table("FinalTable").count
 
     if(resultDF!=spark.emptyDataFrame){  
       resultDF.write.format("orc").mode(SaveMode.Overwrite).option("truncate", true).save(HDFS_Connection.getUri().toString()+HDFS_Location+"/"+tableName)
     }
      
     resultDF
   }
   
   
   def CastColumn(spark: SparkSession, input_df: DataFrame, ColumnName: String, datatype: String): DataFrame = {
     
     var outputDF = spark.emptyDataFrame
     
     println(ColumnName)
     println(ColumnName==null)
     
     if(ColumnName!=null){
     
       if(datatype=="Timestamp"){
         outputDF = input_df.withColumn(ColumnName, col(ColumnName).cast(TimestampType))
       }
       else if(datatype=="Integer"){
         outputDF = input_df.withColumn(ColumnName, col(ColumnName).cast(IntegerType))
       }       
       else if(datatype=="Boolean"){
         outputDF = input_df.withColumn(ColumnName, col(ColumnName).cast(BooleanType))
       }
       else if(datatype=="String"){
         outputDF = input_df.withColumn(ColumnName, col(ColumnName).cast(StringType))
       }      
       else if(datatype=="Date"){
         outputDF = input_df.withColumn(ColumnName, col(ColumnName).cast(DateType))
       }
                  
       //outputDF.printSchema()
       outputDF
     }
     else{      
       input_df
     }
                     
   }
   
   def PurgeHealthTableRecord(spark: SparkSession,Health_DF:DataFrame,ID_FK: Int, connectnPro: Properties, tableName: String){
     
     import spark.implicits._
     
     var resultDF = spark.emptyDataFrame    
     resultDF = Health_DF.filter($"ID_FK"!==ID_FK)
     resultDF.createOrReplaceTempView("FinalTable")
     spark.table("FinalTable").count //force action
     
     if(resultDF!=spark.emptyDataFrame){  
       
       val jdbcUrl = connectnPro.getProperty("jdbcUrl")
       val userid = connectnPro.getProperty("user")
       val password = connectnPro.getProperty("password")
       
       spark.table("FinalTable").write.mode(SaveMode.Overwrite).option("truncate", true).jdbc(url = jdbcUrl, table = tableName,connectnPro) //replace("SYSMAN.","")
     }
          
   }
    
      
}

