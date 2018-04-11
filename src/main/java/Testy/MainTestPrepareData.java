package Testy;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

import java.util.Scanner;

/**
 * Created by as on 11.04.2018.
 */
public class MainTestPrepareData {
    public static void main(String[] args) {
        // INFO DISABLED
//        Logger.getLogger("org").setLevel(Level.OFF);
//        Logger.getLogger("akka").setLevel(Level.OFF);
//        Logger.getLogger("INFO").setLevel(Level.OFF);

//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Experiment_Implementation_Kmeans")
//                //.set("spark.driver.allowMultipleContexts", "true")
//                //.set("spark.eventLog.enabled", "true")
//                .setMaster("local");

        SparkConf conf = new SparkConf()
                .setAppName("Spark_Experiment_Implementation_Kmeans")
                .setMaster("spark://10.2.28.17:7077")
                .setJars(new String[] { "out/artifacts/SparkProject_jar/SparkProject.jar" })
                .set("spark.executor.memory", "15g")
                .set("spark.driver.host", "10.2.28.31");

        SparkContext sc = new SparkContext(conf);
        SparkSession spark = new SparkSession(sc);


        String path = "hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kmean.txt";
        //String path = "data/mllib/kdd_10_proc.txt.gz";
        //String path = "data/mllib/kmean.txt";
        //String path = "data/mllib/iris.csv";
        //String path = "hdfs:/192.168.100.4/data/mllib/kmean.txt";

        // load mem data
        MemDataSet memDataSet = new MemDataSet(spark);
        memDataSet.loadDataSet(path);

        DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> ds = dpc.prepareDataset(memDataSet.getDs(), false);//.select("features");
        ds.show();
        ds.printSchema();
        System.out.println(ds.count());
        spark.close();
        //new Scanner(System.in).nextLine();
    }
}
