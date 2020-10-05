package com.imam;
import java.io.File;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class datatohive {
    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
        .builder()  
        .appName("Data Spotify")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate();

        Dataset<Row> datasetFile = spark.read()
        .format("csv") 
        .option("delimiter", ",")
        .option("inferSchema", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("header", "true") 
        .load("/home/munawar_imam9/data.csv");

        Dataset<Row> data_bersih = datasetFile.withColumn("name", regexp_replace(col("name"), "[^\\p{L}\\p{N}\\p{P}\\p{Z}]", "")) 
                                .withColumn("artists", regexp_replace(col("artists"), "'", ""))
                                .withColumn("artists", regexp_replace(col("artists"), "[\\[\\]\\']", ""));  
        
        datatohive hive = new datatohive();
        hive.dataspotify(data_bersih);
    }

    private void dataspotify(Dataset<Row> data) {
        Dataset<Row> alldata = data.select("*");
        alldata.write().mode("overwrite").format("parquet").saveAsTable("spotify.dataspotify");
    }
}
