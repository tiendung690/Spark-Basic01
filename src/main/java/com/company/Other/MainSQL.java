package com.company.Other;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;


/**
 * Created by as on 13.12.2017.
 */
public class MainSQL {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Spark_JDBC")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

        Dataset<Row> jdbcDF = sparkSession.read()
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/rsds_data")
                .option("dbtable", "slowa_kluczowe")
                .option("user", "root")
                .option("password", "")
                .format("org.apache.spark.sql.execution.datasources.jdbc.DefaultSource")
                .load();

//        Dataset<Row> jdbcDF2 = jdbcDF.withColumn("id", functions.row_number().over(Window.orderBy("slowo_klucz")));
        Dataset<Row> jdbcDF2 = jdbcDF.withColumn("id", functions.monotonically_increasing_id());

        jdbcDF2.printSchema();

        jdbcDF.printSchema();
        jdbcDF2.show(20);
        System.out.println(jdbcDF.count());  //Mozliwość sprawdzenia ile jest wierszy w tablicy

        System.out.println(jdbcDF.columns().length); // liczba kolumn dataseta

        System.out.println(jdbcDF.first().get(0).getClass()); // pobiera pierwszy wiersz

        System.out.println(jdbcDF.columns()[0]); // wyswietla nazwe kolumny 0


        System.out.println(jdbcDF.collectAsList().get(0));
        System.out.println(jdbcDF.collectAsList().get(1));

        System.out.println(jdbcDF.rdd().take(1));
        System.out.println("**************: " + jdbcDF2.select());

        Iterator<Row> iter = jdbcDF2.toLocalIterator();
        System.out.println("-------- " + iter.next());
        System.out.println("-------- " + iter.next());
        System.out.println("-------- " + iter.next());
        System.out.println("schema" + jdbcDF2.first().schema());


        jdbcDF2.filter(jdbcDF2.col("id").equalTo(2000)).show();
        // to samo, taki sam plan dzialania
        // SELECT * FROM `slowa_kluczowe` LIMIT 200,1 // sparwdzenie poprawnosci




//        final DataFrame withoutCurrency = sqlContext.createDataFrame(somedf.javaRDD().map(row -> {
//            return RowFactory.create(row.get(0), row.get(1), someMethod(row.get(2)));
//        }), somedf.schema());


//                try {
//            jdbcDF2.createGlobalTempView("people");
//        } catch (AnalysisException e) {
//            e.printStackTrace();
//        }
//         sparkSession.sql("SELECT * FROM global_temp.people WHERE id=2000").show();

//        while(iter.hasNext()){
//            System.out.println(iter.next());
//        }
//        Dataset<Row> jdbcDF2 = jdbcDF.cache();
//        System.out.println("xxxxxxxx "+jdbcDF2.toLocalIterator().next());
//        System.out.println("xxxxxxxx "+jdbcDF2.toLocalIterator().next());

        //System.out.println(jdbcDF.collectAsList().toLocalIterator().next());

        //Iterator<Row> iter = jdbcDF.toLocalIterator();   // ZA WOLNO BD DZIALAC


        //System.out.println(jdbcDF.logicalPlan());


    }
}
