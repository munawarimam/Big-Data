package com.imam;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;
// import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import static org.apache.spark.sql.functions.*;
import java.io.File;

public class structuredstream_readkafka {
    public static void main(String[] args) throws StreamingQueryException, InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession.builder()
        .master("local")
        .appName("Streaming Twitter")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .config("spark.driver.memory", "10g")
        .config("spark.executor.memory", "10g")
        .enableHiveSupport()
        .getOrCreate();

        spark.sql("create table bigproject.tweetspotify(date string, username string, "
        + "urls string, id_song string) row format delimited fields terminated by ',' "
        + "tblproperties(skip.header.line.count = 1)");

        Dataset<Row> df = spark.readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "spotify").option("startingOffsets", "earliest")
        .option("fetch.max.bytes", "52428800").option("max.partition.fetch.bytes", "1048576")
        .load()
        .selectExpr("CAST(value AS STRING)");

        Dataset<Row> datasplit = df.selectExpr("split(value, '[|]')[0] as date", 
        "split (value, '[|]')[1] as username",
        "split (value, '[|]')[2] as urls");

        Dataset<Row> databersih = datasplit.withColumn("username", regexp_replace(col("username"), "'", ""))
        .withColumn("username", regexp_replace(col("username"), "^.", ""))
        .withColumn("id_song", regexp_extract(col("urls"), "\\/track\\/([a-zA-Z0-9]+)(.*)$", 1));

        // StreamingQuery test = databersih
        // .writeStream()
        // .outputMode("update")
        // .format("console")
        // .start();
        // test.awaitTermination();
        
        StreamingQuery testing = databersih.writeStream()
        .outputMode("append")
        .format("csv")
        .option("checkpointLocation", "/user/hive/warehouse/streamingtweet")
        .option("path", "hdfs://bigproject-m/user/hive/warehouse/spotify.db/tweetspotify")
        .option("truncate", "false")
        .option("failOnDataLoss", "false")
        .start();
        testing.awaitTermination();
        
    }   
}
