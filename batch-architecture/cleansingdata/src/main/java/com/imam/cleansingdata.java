package com.imam;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import java.io.File;
public class cleansingdata {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // Create spark session
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
        .builder()
        .appName("Project Spark SQL Hive Airflow")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate();

        // Read data dari data scrappingtweet
        Dataset<Row> df1 = spark.read()
        .format("csv") 
        .option("delimiter", ",")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("header", "true") 
        // ini merupakan lokasi dimana data tweet yang dicrawling disimpan
        .load("file:///home/munawarimam/COVID19"); 

        // Cleansing data
        Dataset<Row> data_bersih = df1.withColumn("tweet", regexp_replace(col("tweet"), "[^\\p{L}\\p{N}\\p{P}\\p{Z}]", "")) //hanya mengambil karakter all letters, number, punctuation, dan whitespace separator
                                .withColumn("username", regexp_replace(col("username"), "'", "")) // menghapus spesial karakter yaitu '
                                .withColumn("username", regexp_replace(col("username"), "^.", "")) //menghapus karakter awal pada value
                                .withColumn("all_hashtags", regexp_replace(col("all_hashtags"), "[\\[\\]\\']", "")); // menghapus kurung siku []   
        
        // data_bersih.createOrReplaceTempView("data_tweet");
        // Dataset<Row> panggil = spark.sql("select * from data_tweet");
       
        cleansingdata hive = new cleansingdata();
        hive.alldata_covid19(data_bersih);
        // membuat dataset memanggil semua row dari tabel data_covid19
        Dataset<Row> alldata = spark.sql("select * from project3.data_covid19");
        
        // cleansing data duplicate dari alldata_covid19
        Dataset<Row> data_noduplicate = alldata.dropDuplicates("date", "username", "languange");
        
        hive.databersih_covid19(data_noduplicate);
        hive.total_tweet_id(data_noduplicate);
        hive.highest_lang_tweet(data_noduplicate);
        hive.time_highest_tweet_us(data_noduplicate);
        
    }

    // method membuat tabel data_covid19 yang akan ditampung di hive
    private void alldata_covid19(Dataset<Row> data) {
        Dataset<Row> tweet = data.select("*");
        tweet.write().mode("append").format("parquet").saveAsTable("project3.data_covid19");
    }

    private void databersih_covid19(Dataset<Row> data) {
        Dataset<Row> tweet = data.select("*");
        tweet.write().mode("overwrite").format("parquet").saveAsTable("project3.databersih_covid19");
    }

    private void total_tweet_id(Dataset<Row> data) {
        Dataset<Row> tweet = data.select("languange")
        .where("languange = 'in'")
        .groupBy("languange").count()
        .withColumnRenamed("count", "jumlah_orang");
        
        tweet.write().mode("overwrite").format("parquet").saveAsTable("project3.total_tweet_id");
    }

    private void highest_lang_tweet(Dataset<Row> data) {
        Dataset<Row> tweet = data.select("languange")
        .groupBy("languange").count()
        .orderBy(desc("count"))
        .limit(1)
        .withColumnRenamed("count", "total");
        
        tweet.write().mode("overwrite").format("parquet").saveAsTable("project3.highest_lang_tweet");
    }
    
    private void time_highest_tweet_us(Dataset<Row> data) {
        Dataset<Row> tweet = data.select("date", "languange")
        .withColumn("tanggal", to_date(col("date")))
        .where("languange = 'en'")
        .groupBy("tanggal", "languange").count()
        .orderBy(desc("count"))
        .limit(1)
        .withColumnRenamed("count", "total");
        
        tweet.write().mode("overwrite").format("parquet").saveAsTable("project3.time_highest_tweet_us");
    }   
}
