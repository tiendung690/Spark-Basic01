package com.company.Other;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by as on 02.01.2018.
 */
public class StringIndexerExample {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);



        SparkConf conf = new SparkConf()
                .setAppName("JavaWordCount")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
        // create Spark Context
        SparkContext context = new SparkContext(conf);
        // create spark Session
        SparkSession sparkSession = new SparkSession(context);

        Dataset<Row> df = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/mllib/iris.csv");




    }
}
