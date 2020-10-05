package com.imam;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;
import java.io.File;

public class jointable {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession.builder()
        .master("local")
        .appName("jointable")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate();

        Dataset<Row> datajoin = spark.sql("select b.id, b.artists, b.name, " +
        "(select count(*) from bigproject.tweetspotify a where a.id_song = b.id) as total " +
        "from bigproject.dataspotify b order by total desc");
        datajoin.show();

        jointable hive = new jointable();
        hive.popularsong(datajoin);

    }
    private void popularsong(Dataset<Row> data) {
        Dataset<Row> alldata = data.select("*");
        alldata.write().mode("overwrite").format("parquet").saveAsTable("bigproject.popularsong");
    }
}