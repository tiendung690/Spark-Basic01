package Testy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by as on 10.03.2018.
 */
public class Testy {
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
       // JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        String data_path = "data/mllib/koszyk.txt";
        Dataset<Row> df = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(data_path);
                //.limit(0);

        df.printSchema();
        System.out.println(df.count());


        StringIndexer stringIndexer = new StringIndexer();
        stringIndexer.setInputCol("kolor").setOutputCol("kolor2");
        stringIndexer.setHandleInvalid("keep");
        stringIndexer.fit(df).transform(df).show();








//         //ZLICZANIE UNIKALNYCH WARTOSCI W KOLUMNACH
//        String[] cols = df.columns();
//        for (String col: cols) {
//            System.out.println(col+" : "+df.select(col).distinct().count());
//        }






        //System.out.println(Arrays.deepToString(df.schema().fields()));
//        for (StructField o : df.schema().fields()) {
//            System.out.println(o.toString());
//        }

//        df.write()
//                .option("driver", "com.mysql.jdbc.Driver")
//                .option("url", "jdbc:mysql://localhost:3306/rsds_data?rewriteBatchedStatements=true") // ?rewriteBatchedStatements=true
//                .option("dbtable", "4slowa_kluczowe")
//                .option("user", "root")
//                .option("password", "")
//                .option("inferSchema", true)
//                .format("org.apache.spark.sql.execution.datasources.jdbc.DefaultSource")
//                .mode(SaveMode.Append)
//                .save();


//        Dataset<Row> jdbcDF = sparkSession.read()
//                .option("driver", "com.mysql.jdbc.Driver")
//                .option("url", "jdbc:mysql://localhost:3306/rsds_data")
//                .option("dbtable", "2slowa_kluczowe")
//                .option("user", "root")
//                .option("inferSchema", false)
//                .option("password", "")
//                .format("org.apache.spark.sql.execution.datasources.jdbc.DefaultSource")
//                .load()
//                .limit(10);
//
//        jdbcDF.printSchema();
//        jdbcDF.show();

        System.out.println("--------------------------------------");

//        Dataset<Row> df2 = sparkSession.createDataFrame(new ArrayList<Row>(), new StructType()
//                .add("IntegerType", DataTypes.IntegerType, true)
//                .add("BooleanType", DataTypes.BooleanType, true)
//                .add("BinaryType", DataTypes.BinaryType, true)
//                .add("ByteType", DataTypes.ByteType, true)
//                .add("DateType", DataTypes.DateType, true)
//                .add("DoubleType", DataTypes.DoubleType, true)
//                .add("FloatType", DataTypes.FloatType, true)
//                .add("LongType", DataTypes.LongType, true)
//                .add("NullType", DataTypes.NullType, true)
//                .add("ShortType", DataTypes.ShortType, true)
//                .add("StringType", DataTypes.StringType, true)
//                //.add("CalendarIntervalType", DataTypes.CalendarIntervalType, true)
//                .add("TimestampType", DataTypes.TimestampType, true));
//
//
//        df2.printSchema();
//
//        df2.write()
//                .option("driver", "com.mysql.jdbc.Driver")
//                .option("url", "jdbc:mysql://localhost:3306/rsds_data?rewriteBatchedStatements=true") // ?rewriteBatchedStatements=true
//                .option("dbtable", "5typy")
//                .option("user", "root")
//                .option("password", "")
//                .format("org.apache.spark.sql.execution.datasources.jdbc.DefaultSource")
//                .mode(SaveMode.Append)
//                .save();


    }
}
