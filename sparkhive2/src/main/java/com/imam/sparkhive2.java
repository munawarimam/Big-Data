package com.imam;

import java.io.File;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class sparkhive2 {
    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
        .builder()  
        .appName("Data E-Commerce")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate();

        Dataset<Row> ecommerceData = spark.read()
        .format("csv") 
        .option("delimiter", ",")
        .option("inferSchema", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("header", "true") 
        .load("file:///home/munawar_imam9/datakotor.csv");
 
        Dataset<Row> data_bersih = ecommerceData.withColumn("description", upper(col("description")))
                            .withColumn("description", trim(col("description")))
                            // change double or multiple spaces to be one space
                            .withColumn("description", regexp_replace(col("description"), "\\s+", " "))
                            // replace punctuation in last character
                            .withColumn("description", regexp_replace(col("description"), "[.,]$", ""))
                            .dropDuplicates("invoiceNo", "stockcode", "description", "invoicedate", "customerid")
                            // change format column
                            .withColumn("invoicedate", unix_timestamp(col("invoicedate"), "MM/dd/yyyy HH:mm").cast(DataTypes.TimestampType));

        sparkhive2 hive = new sparkhive2();
        hive.popularItem(data_bersih);
        hive.popularStock(data_bersih);
        hive.popularPurchasedDate(data_bersih);
        hive.popularPurchasedCountry(data_bersih);
        hive.allValueByLowestSales(data_bersih);
    }

    private void popularItem(Dataset<Row> data) {
        Dataset<Row> item = data.select("description", "quantity")
        .groupBy("description").sum("quantity")
        .orderBy(desc("sum(quantity)"))
        .limit(1)
        .withColumnRenamed("sum(quantity)", "jumlah");

        item.write().mode("overwrite").format("orc").saveAsTable("project_ecommerce.popular_item");
    }

    private void popularStock(Dataset<Row> data) {
        Dataset<Row> stock = data.select("stockCode", "quantity")
        .groupBy("stockcode").sum("quantity")
        .orderBy(desc("sum(quantity)"))
        .limit(1)
        .withColumnRenamed("sum(quantity)", "jumlah");

        stock.write().mode("overwrite").format("orc").saveAsTable("project_ecommerce.popular_stock");
    }

    private void popularPurchasedDate(Dataset<Row> data) {
        Dataset<Row> date = data.select("invoicedate", "quantity")
        .withColumn("tanggal", to_date(col("invoicedate")))
        .where("stockcode = '84077'")
        .groupBy("tanggal").sum("quantity")
        .orderBy(desc("sum(quantity)"))
        .limit(1)
        .withColumnRenamed("sum(quantity)", "jumlah");

        date.write().mode("overwrite").format("orc").saveAsTable("project_ecommerce.popular_purchased_date");
    }

    private void popularPurchasedCountry(Dataset<Row> data) {
        Dataset<Row> country = data.select("country", "quantity")
        .where("stockcode = '84077'")
        .groupBy("country").sum("quantity")
        .orderBy(desc("sum(quantity)"))
        .limit(1)
        .withColumnRenamed("sum(quantity)", "jumlah");

        country.write().mode("overwrite").format("orc").saveAsTable("project_ecommerce.popular_purchased_country");
    }

    private void allValueByLowestSales(Dataset<Row> data) {
        Dataset<Row> minitem = data.select("country",  "quantity")
        .where("description = 'WORLD WAR 2 GLIDERS ASSTD DESIGNS'")
        .groupBy("country").sum("quantity")
        .orderBy(asc("sum(quantity)"))
        .limit(1)
        .withColumnRenamed("sum(quantity)", "jumlah");

        minitem.write().mode("overwrite").format("orc").saveAsTable("project_ecommerce.lowest_sales");
    }
}
