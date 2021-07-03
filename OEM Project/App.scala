////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       Created By: Suraj Patil                                          //
//                                          M. +91 9167762748                                             //
//                                      Mail ID: suraj3.patil@ril.com                                     //
//                                          Date: 26-May-2021                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////////////

package com.jio.oem

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.jio.utilities._
import com.jio.utilities.ReadInput._
import com.jio.utilities.StoreOutput._
import java.text.SimpleDateFormat
import java.util.Properties
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window
import java.sql.Timestamp
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import com.jio.encrypt.DES3._
import org.apache.hadoop.conf.Configuration
import java.util.Date
import scala.util.Try

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.io.{BufferedOutputStream,BufferedInputStream}

//import java.sql._
import java.util.Properties
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions


object oem_data_processing {
 
  //System.setProperty("hadoop.home.dir","Z:\\CommonRepo\\winutils")
  
  // connect Eclipse to HDFS
  def get_filesystem(): FileSystem = {
    
    val conf = new Configuration
    conf.addResource(new Path("Z:\\Dinesh_Patil\\DP\\hdfs_conf\\core-site.xml"))
    conf.addResource(new Path("Z:\\Dinesh_Patil\\DP\\hdfs_conf\\hdfs-site.xml"))

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val fs = FileSystem.get(conf)
    
    fs
  } 
                   
  def main(args: scala.Array[String]): Unit = { //scala.Array to avoid conflict with java.sql.Array
   
    val conf = new SparkConf().setAppName("OEM_Data_Processing").setMaster("yarn") // local[*] and yarn
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //val sc = new SparkContext(conf)
    //val ssc = new StreamingContext(sc, Seconds(15))

    import spark.implicits._
    spark.sparkContext.setLogLevel("Error")
    
    // Reading config file
    val conf_data = JsonParser.parseConfigJson(spark, args(0)) // "/user/OEM_conf.json" or "Z:\Suraj3.Patil\workspace\OEM\resources\OEM_conf.json"
    val JobName = conf_data("jobName").asInstanceOf[String] // "OEM_Data_Processing"
    val gg_conf_master = conf_data("gg_conf_master").asInstanceOf[String] // "ngo_gg_replication_master"
    val gg_conf_table_master = conf_data("gg_conf_table_master").asInstanceOf[String] // "ngo_gg_replication_table_master"
    val gg_conf_health=conf_data("gg_conf_health").asInstanceOf[String] // "ngo_gg_replication_health_log"
    val RefreshInterval=conf_data("RefreshInterval").asInstanceOf[String].toInt // "4"
    val config_url_value = conf_data("config_url_value").asInstanceOf[String] // "jdbc:oracle:thin:@10.143.182.153:1521/NJIODEV"
    val config_user_name = conf_data("config_user_name").asInstanceOf[String] // "SIJIODB"
    val config_password = decryptKey(conf_data("config_password").asInstanceOf[String]) 
    val driver_value = conf_data("driver_value").asInstanceOf[String]
    val kafka_topic_list = conf_data("kafka.topic.list").asInstanceOf[String].split(',') // this is in table as well
    val bootstrap_servers = conf_data("bootstrap.servers").asInstanceOf[String]
    val zookeeper_connect = conf_data("zookeeper.connect").asInstanceOf[String]
    val key_deserializer = conf_data("key.deserializer").asInstanceOf[String]
    val value_deserializer = conf_data("value.deserializer").asInstanceOf[String]
    val group_id = conf_data("group.id").asInstanceOf[String]
    val enable_auto_commit = conf_data("enable.auto.commit").asInstanceOf[String]
    val request_timeout_ms = conf_data("request.timeout.ms").asInstanceOf[String]
    val session_timeout_ms = conf_data("session.timeout.ms").asInstanceOf[String]
    val fetch_max_wait_ms = conf_data("fetch.max.wait.ms").asInstanceOf[String]
    val zookeeper_connection_timeout_ms = conf_data("zookeeper.connection.timeout.ms").asInstanceOf[String]
    val StreamingInterval = conf_data("StreamingInterval").asInstanceOf[String].toInt
    
    // For Error Logging and Health Logging
    val ngo_error_table = conf_data("ngo_error_table").asInstanceOf[String]
    val ngo_health_table = conf_data("ngo_health_table").asInstanceOf[String]
        
    if(spark.catalog.tableExists("ngo_gg_replication_master")) {
        spark.catalog.cacheTable("ngo_gg_replication_master")
      }
    if(spark.catalog.tableExists("ngo_gg_replication_table_master")) {
        spark.catalog.cacheTable("ngo_gg_replication_table_master")
      }
    
    // Reading from ngo_gg_replication_health_log table
    val healthconf = spark.read.format("jdbc")
      .option("url", config_url_value)
      .option("dbtable", gg_conf_health)
      .option("user", config_user_name)
      .option("password", config_password)
      .option("driver", driver_value).load.cache
          
    // Reading from ngo_gg_replication_master
    val master_conf = spark.read.format("jdbc")
    .option("url", config_url_value)
    .option("dbtable", gg_conf_master)
    .option("user", config_user_name)
    .option("password", config_password)
    .option("driver", driver_value).load.cache
        
    // Reading from ngo_gg_replication_table_master
    val master_table_conf = spark.read.format("jdbc")
    .option("url", config_url_value)
    .option("dbtable", gg_conf_table_master)
    .option("user", config_user_name)
    .option("password", config_password)
    .option("driver", driver_value).load.cache
    
    
      
    /* Spark Streaming Job Begins */
    
    // Create Streaming Context
    val ssc = new StreamingContext(spark.sparkContext, Seconds(StreamingInterval))
                          
    // Reading Kafka topic from table
    val master_conf_job = master_conf.filter($"JOBNAME"===JobName)
    //var topics = master_conf_job.select("Input_Kafka_topic").collect.map(_.getString(0)) //convert to array
       
    master_conf_job.show()
    
    //println(topics.mkString, kafka_topic_list.mkString)

    //If topics in config and table don't match, job must restart after change to config
    // assert(topics.deep == kafka_topic_list.deep)  
    
    // Define all the kafka properties for streaming
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> bootstrap_servers,
        "key.deserializer" -> key_deserializer,
        "value.deserializer" -> value_deserializer,
        "group.id" -> group_id,
        //"auto.offset.reset" -> "latest",
        "enable.auto.commit" -> enable_auto_commit
        )
        
    val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](kafka_topic_list, kafkaParams))
        
    var stream_start_time:String = null //  For refreshing tables
        
    stream.foreachRDD(rdd => {                
      if (!rdd.isEmpty()) {
        try {
          val format = new SimpleDateFormat("dd-MMM-yyyy hh:mm.ss a")
          val start = spark.sql("select from_unixtime(unix_timestamp(current_timestamp,'yyyy-MM-dd HH:mm:ss'),'dd-MMM-yyyy hh:mm.ss a') end").collect
          val startTimeString = start(0).toString.substring(1, start(0).toString.length - 1)
          val startTimeInMilliSeconds = format.parse(start(0).toString.replace("[", "").replace("]", "")).getTime
                       
          val inputValues = rdd.map(record => record.value())
          var inputDS = spark.read.json(inputValues)
          inputDS.cache
          
          val time_format= "HH:mm:ss"         
          if(stream_start_time==null){
            stream_start_time = startTimeString.substring(12,20).replace(".",":") // 31-May-2021 05:29.00 PM
            
            val AM_or_PM = stream_start_time.takeRight(2)
            
            if(AM_or_PM == "PM"){
              val hours = stream_start_time.substring(2)
              val new_hours = hours.toInt + 12
              val string_hours = new_hours.toString
              stream_start_time = string_hours + stream_start_time.substring(2,8)
            }
          }
          val curr_time = getDate(time_format)
          
          // Check if Tables need to be refreshed
          if(CheckTableRefresh(stream_start_time, curr_time, RefreshInterval)) {
            if(spark.catalog.tableExists("ngo_gg_replication_master")){
              spark.catalog.uncacheTable("ngo_gg_replication_master") 
            }
            if(spark.catalog.tableExists("ngo_gg_replication_table_master")){
              spark.catalog.uncacheTable("ngo_gg_replication_table_master")
            }
          }
       
          // Process data sequentially for each table (seq = 1 followed by seq = 2) 
          
          //var master_df = master_conf_job.join(master_table_conf,master_table_conf("ID_FK")===master_conf_job("id"))
          //                               .join(healthconf,healthconf("ID_FK")===master_conf_job("id"))
                                         
                                        
          //master_df = master_df.orderBy("Sequence")
                   
          var master_df = master_conf_job.orderBy("Sequence") //.filter($"ID"===4) // filter just for dev testing
                                  
          //var master_table_df = master_table_conf.orderBy("Sequence")
          //val Sequence_list = master_conf_job.select("Sequence").collect.map(_.getString(0)) //convert to array
         
          val iterable=master_df.collect.map(r => Map(master_df.columns.zip(r.toSeq):_*))
          
          for(i <- iterable){              
            //val Id = iterable.getAs[Int]("ID").toString
            val Id = i("ID").asInstanceOf[java.lang.Number].intValue
            val Connection_String = i("CONNECTIONSTRING").asInstanceOf[String] // "jdbc:oracle:thin:@hostname:port:name/servicename" or "hdfs://yourURL:/port"
            val tablename_tag_topic = i("TABLENAME_TAG_IN_TOPIC").asInstanceOf[String]
            val tablename_table = i("TABLENAME_IN_TABLE_TAG").asInstanceOf[String]
            val tablename_DB = i("TABLENAME_IN_DB").asInstanceOf[String]
            val PrimaryKey = i("PRIMARYKEY").asInstanceOf[String]
            val HDFSLocation1 = i("HDFSLOCATION1").asInstanceOf[String] // "/path/to/file/"
            val HDFSLocation2 = i("HDFSLOCATION2").asInstanceOf[String]
            val Ot_type = i("OTTYPE_TAG_IN_KAFKA").asInstanceOf[String]
            val Purging = i("PURGING").asInstanceOf[String]         // Y or N
            val Purging_days = i("PURGING_DAYS").asInstanceOf[java.math.BigDecimal]
            val Purging_column = i("PURGING_COLUMN").asInstanceOf[String]
            
            val Input_Kafka_Topic = i("INPUT_KAFKA_TOPIC").asInstanceOf[String].split(",")
            
            assert(Input_Kafka_Topic.deep==kafka_topic_list.deep)
            
            //val LAST_RUN_TIME = healthconf.filter($"id_fk"===Id).collect().head.getAs[org.apache.spark.sql.types.TimestampType]("LAST_RUN_TIME")
            
            //val LAST_RUN_TIME = healthconf.filter($"id_fk"===Id).collect().head.getAs[java.util.Date]("LAST_RUN_TIME")                                    
            val LAST_RUN_TIME = healthconf.filter($"id_fk"===Id).collect().head.getAs[java.sql.Timestamp]("LAST_RUN_TIME")
                                            
            val database_name = Connection_String.split("\\/")(1) //service name is Database name
            val HDFS_flag = if (Connection_String.substring(0,7).equals("hdfs://")) true else false
                       
            // Filtering records by ColumnName from config table
            val Column_String = master_table_conf.filter($"id_fk"===Id).collect().head.getAs[String]("COLUMNNAME")
            val Datatype_String = master_table_conf.filter($"id_fk"===Id).collect().head.getAs[String]("DATATYPE")
            //var records_df = inputDS.filter($"Column"===Column_String) 
                                    
            // Get Input and Output datetime format
            val Input_datetime_format = master_table_conf.filter($"id_fk"===Id).collect().head.getAs[String]("INPUT_DATETIME_FORMAT")
            val Output_datetime_format = master_table_conf.filter($"id_fk"===Id).collect().head.getAs[String]("OUTPUT_DATETIME_FORMAT")
                                              
            var records_df = spark.emptyDataFrame
            records_df = inputDS.filter(col(tablename_tag_topic)===tablename_table)                       
            var retrieved_table_df = spark.emptyDataFrame
            

            var HDFS_Connection:FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration) // use below line in dev
            //var HDFS_Connection:FileSystem = get_filesystem()                       
            
            var Fileexists_Location1: Boolean = false // Boolean can only be true or false (not null)
            var Fileexists_Location2: Boolean= false
            
            var JDBC_Connection = new Properties
            
            //println(Fileexists_Location1,Fileexists_Location2)
            
            
            // Check if records for the given table/file exist
            if(records_df.count!=0){
       
              // Check if it is HDFS file
              if(HDFS_flag){
                //var HDFS_Connection = FileSystem.get(new URI(Connection_String), new Configuration)
                var Fileexists_Location1= if(HDFS_Connection.exists(new Path(HDFSLocation1+"/"+tablename_table))) true else false
                var Fileexists_Location2= if(HDFS_Connection.exists(new Path(HDFSLocation2+"/"+tablename_table))) true else false
                
                //println(Fileexists_Location1,Fileexists_Location2)
                println(HDFS_Connection.getUri().toString()+HDFSLocation1+"/"+tablename_table)   
                
                //var in: BufferedInputStream = null
                
                if(Fileexists_Location1){
                    retrieved_table_df = spark.read.load(HDFS_Connection.getUri().toString()+HDFSLocation1+"/"+tablename_table)                   
                  }
                else if(Fileexists_Location2){
                    retrieved_table_df = spark.read.load(HDFS_Connection.getUri().toString()+HDFSLocation2+"/"+tablename_table)
                  }
              }
              // If not HDFS file, then it is Oracle table
              else{
                               
                JDBC_Connection.setProperty("jdbcUrl", Connection_String)
                JDBC_Connection.setProperty("user", config_user_name) // <- put appropriate credentials here (from connection string?)
                JDBC_Connection.setProperty("password", config_password)
                JDBC_Connection.setProperty("driver", driver_value)
                JDBC_Connection.setProperty("dbname", "database_name")
                            
                retrieved_table_df = spark.read.format("jdbc")
                .option("url", Connection_String)
                .option("dbtable", tablename_table)
                .option("user", config_user_name) // <- put appropriate credentials here (from connection string?)
                .option("password", config_password)
                .option("driver", driver_value).load.cache    
                
                retrieved_table_df.count //to force an action
                
                // For Testing Purposes               
                             
                //retrieved_table_df.write.mode("overwrite").csv("Z:\\Suraj3.Patil\\workspace\\OEM\\target\\"+tablename_table)                    
                //retrieved_table_df.write.format("orc").mode(SaveMode.Overwrite).option("truncate", true).save(HDFS_Connection.getUri().toString()+HDFSLocation1+"/"+tablename_table)    
              }                      
           
            // Check Purging               
              if(Purging=="Y"){
                
                 if(HDFS_flag){
                   if(Fileexists_Location1){
                      retrieved_table_df= UpdateTables.purgeHDFSFiles(spark,retrieved_table_df,HDFS_Connection,tablename_table,HDFSLocation1,PrimaryKey,Purging_days,Purging_column)
                   }
                   else if(Fileexists_Location2){
                      retrieved_table_df= UpdateTables.purgeHDFSFiles(spark,retrieved_table_df,HDFS_Connection,tablename_table,HDFSLocation2,PrimaryKey,Purging_days,Purging_column)
                   }
                 }                   
                 else{
                   retrieved_table_df= UpdateTables.purgeOracleTables(spark,retrieved_table_df,JDBC_Connection,tablename_table,PrimaryKey,Purging_days,Purging_column)
                 }                
              }
                          
              // Check if TableName_in_DB is not null 
              if(tablename_DB!=null){
              
                // Overwrite the final processed data in Oracle table
                Class.forName(driver_value)
                val sc = spark.sparkContext                 
                val brConnect = sc.broadcast(JDBC_Connection) //broadcast jdbc connection parameters to each partition
                UpdateTables.updateOracleDB(spark,retrieved_table_df,records_df,JDBC_Connection,tablename_table,PrimaryKey,Ot_type,Column_String,Datatype_String)
              }
            
              else{                                           
                // At the beginning, there is no data in both so write in any one of them                                   
                if(!Fileexists_Location1 && !Fileexists_Location2){
                  UpdateTables.updateHDFSFiles(spark,retrieved_table_df,records_df,HDFS_Connection,HDFSLocation1,tablename_DB,PrimaryKey,Ot_type,Column_String,Datatype_String) 
                }   
              
                // Overwrite the final processed data into the HDFS Location which was not having data
                else if(!Fileexists_Location1){
                  UpdateTables.updateHDFSFiles(spark,retrieved_table_df,records_df,HDFS_Connection,HDFSLocation1,tablename_DB,PrimaryKey,Ot_type,Column_String,Datatype_String) 
                }
                else if(!Fileexists_Location2){
                  UpdateTables.updateHDFSFiles(spark,retrieved_table_df,records_df,HDFS_Connection,HDFSLocation2,tablename_table,PrimaryKey,Ot_type,Column_String,Datatype_String) 
                }              
              }
           }
            
              // Purge Records from Health Log Table after 15 days                          
              val curr_timestamp = getDate("dd-MMM-yyyy hh:mm.ss a")                            
              val current_timestamp = format.parse(curr_timestamp)
              
              val Health_table_Purge = CheckTablePurge(LAST_RUN_TIME, current_timestamp)
              
              if(Health_table_Purge){
                var JDBC_Connection_Health = new Properties
                
                JDBC_Connection_Health.put("jdbcUrl", Connection_String)
                JDBC_Connection_Health.put("user", config_user_name)
                JDBC_Connection_Health.put("password",config_password)  // <- put appropriate credentials here (from connection string?)
                JDBC_Connection_Health.put("driver",driver_value)
                JDBC_Connection_Health.put("dbname",database_name)               
                
                UpdateTables.PurgeHealthTableRecord(spark,healthconf,Id,JDBC_Connection_Health,gg_conf_health)
              }                           
          
              //HDFS_Connection.close() // Close the connection                                
          }
          
          
          
          
          val end = spark.sql("select from_unixtime(unix_timestamp(current_timestamp,'yyyy-MM-dd HH:mm:ss'),'dd-MMM-yyyy hh:mm.ss a') end").collect
          val endTimeString = end(0).toString.substring(1, end(0).toString.length - 1)
          val endTimeInMilliSeconds = format.parse(end(0).toString.replace("[", "").replace("]", "")).getTime
          val totalTimeTaken = endTimeInMilliSeconds - startTimeInMilliSeconds
          
//          println("Writing to ngo_health_table")
//          Utility.HealtLogging(spark, ngo_health_table, url_value ,oracle_user_name , oracle_password, driver_value, "Application", "Application", 
//          spark.sparkContext.getConf.get("spark.app.id"), spark.sparkContext.getConf.get("spark.app.name"),startTimeString, endTimeString, 0, 
//          spark.sparkContext.getConf.get("spark.submit.deployMode"),spark.sparkContext.getConf.get("spark.master"), spark.sparkContext.getConf.get("spark.executor.instances"), 
//          spark.sparkContext.getConf.get("spark.executor.memory"),spark.sparkContext.getConf.get("spark.executor.cores"), totalTimeTaken.toString(), "running")
        } catch{
          case ex: Exception =>{
            println("Error in job" + ex.printStackTrace())
            val stackMessage = ex.getStackTrace.mkString("##")
            val exceptionStack = stackMessage.size match {
              case x if x < 2000 => stackMessage.take(x)
              case x if x > 2000 => stackMessage.take(1990)
              }
          var errorTime = spark.sql("select from_unixtime(unix_timestamp(current_timestamp,'yyyy-MM-dd HH:mm:ss'),'dd-MMM-yyyy hh:mm.ss a') s").collect                
          var errorTimeString = errorTime(0).toString.substring(1, errorTime(0).toString.length - 1)
          
//          println("Writing to ngo_error_table")
//          Utility.LogError(spark, ngo_error_table, conf_data("db.url").asInstanceOf[String],conf_data("db.user").asInstanceOf[String],
//          com.jio.encrypt.DES3.decryptKey(conf_data("db.password").asInstanceOf[String]), conf_data("db.driver").asInstanceOf[String],
//          "Application", "Rule Engine", spark.sparkContext.getConf.get("spark.app.id"), spark.sparkContext.getConf.get("spark.app.name"), 
//          errorTimeString, ex.getMessage, exceptionStack)                          
          }
        }
      }
    })
    
    
          
    ssc.start()
    ssc.awaitTermination()    
    
    /* Spark Streaming Job Ends */       
       
    
  }
  
  
  def CheckTableRefresh(stream_start_time: String, current_time:String, RefreshInterval: Int): Boolean = {
    
      val hour_difference = (current_time.substring(0,2).toInt - stream_start_time.substring(0,2).toInt).abs
      val minute_difference = (current_time.substring(3,5).toInt - stream_start_time.substring(3,5).toInt).abs
      val second_difference = (current_time.substring(6,8).toInt - stream_start_time.substring(3,5).toInt).abs
    
      val time_difference = hour_difference + (minute_difference/60) + (second_difference/60*60)
      if(time_difference > RefreshInterval){
        true
      }
    
    false
  }
  
  def CheckTablePurge(last_run_time: Date, current_time: Date): Boolean = {
    
      println(current_time.toString(),last_run_time.toString())  
      println("current_time",current_time.getTime())
      println("last_run_time",last_run_time.getTime())
      
      val time_difference = current_time.getTime() - last_run_time.getTime()
      
      val day_difference = (time_difference/(1000*60*60*24)) % 365
      
      println("time_difference",time_difference)
      println("day_difference",day_difference)
      
      if(day_difference>=15){
        true
      }
      
      else{
        false
      }
         
    }
    
    
  
}

