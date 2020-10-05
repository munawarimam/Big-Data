package com.imam;

import java.io.File;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class sparkhive {
    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
        .builder()  
        .appName("Data Covid Java Spark")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate();

        spark.sql("create table project_covid.covid2019(iso_code string, continent string, location string, date date, "
        + "total_cases int, new_cases int, total_deaths int, new_deaths int, total_cases_per_million int, "
        + "new_cases_per_million int, total_deaths_per_million int, new_deaths_per_million int, "
        + "new_tests int, total_tests int, total_tests_per_thousand int, new_test_per_thousand int, "
        + "new_tests_smoothed int, new_tests_smoothed_per_thousand int, tests_units string, "
        + "stringency_index int, population int, population_density int, median_age int, aged_65_older int, "
        + "aged_70_older int, gdp_per_capita int, extreme_poverty int, cardiovasc_death_rate int, "
        + "diabetes_prevalence int, female_smokers int, male_smokers int, handwashing_facilities int, "
        + "hospital_beds_per_thousand int, life_expectancy int) row format delimited fields terminated by ',' "
        + "tblproperties(skip.header.line.count = 1)");

        // import data from repo in hdfs
        spark.sql("load data inpath '/user/munawarimam/owid-covid-data.csv' overwrite into table project_covid.covid2019");
        Dataset<Row> allCovid = spark.sql("select * from project_covid.covid2019");

        sparkhive hive = new sparkhive();
        hive.averageNewCases(allCovid);
        hive.highestAdditionalCases(allCovid);
        spark.sql("select * from project_covid.highest_new_cases").show(20);
    }

    private void averageNewCases(Dataset<Row> data) {
        Dataset<Row> res = data.select("location", "new_cases")
        .groupBy("location")
        .avg("new_cases")
        .withColumnRenamed("avg(new_cases)", "rata_rata");

        res.write().mode("overwrite").format("orc").saveAsTable("project_covid.average_new_cases");
    }

    private void highestAdditionalCases(Dataset<Row> data) {
        Dataset<Row> res = data.select("location", "date", "new_cases", "new_deaths", "total_deaths")
        .orderBy(desc("new_cases"), asc("date")) 
        .orderBy(asc("location"))
        .where("new_cases is not null")
        .dropDuplicates("location");

        res.write().mode("overwrite").format("orc").saveAsTable("project_covid.highest_new_cases");
    }
}