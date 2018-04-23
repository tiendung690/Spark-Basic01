package sparktemplate.test;

import myimplementation.Kmns;
import myimplementation.Test1;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.clustering.ClusteringSummary;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.DataRecord;
import sparktemplate.association.AssociationSettings;
import sparktemplate.clustering.ClusteringSettings;
import sparktemplate.clustering.KMean;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Created by as on 13.03.2018.
 */
public class TestClustering2 {
    public static void main(String[] args) throws IOException {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Default-Kmeans")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                .set("spark.driver.memory", "2g")
                .set("spark.executor.memory", "2g")
                //.set("spark.driver.memory", "4g")
                //.set("spark.executor.memory", "4g")
                //.set("spark.default.parallelism", "12")
                .setMaster("local[1]");

//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Default_Kmeans")
//                .setMaster("spark://10.2.28.17:7077")
//                .setJars(new String[] { "out/artifacts/SparkProject_jar/SparkProject.jar" })
//                //.set("spark.executor.memory", "15g")
//                .set("spark.default.parallelism", "12")
//                .set("spark.driver.host", "10.2.28.31");

//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Default_Kmeans")
//                .setMaster("spark://192.168.100.4:7077")
//                .setJars(new String[] { "out/artifacts/SparkProject_jar/SparkProject.jar" })
//                //.set("spark.executor.memory", "15g")
//                //.set("spark.default.parallelism", "12")
//                .set("spark.driver.host", "192.168.100.2");

        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);




        //String path = "hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt.gz";
        String path = "data/mllib/kdd_10_proc.txt";
        //String path = "data/mllib/kmean.txt";
        //String path = "data/mllib/iris2.csv";
        //String path = "data/mllib/creditcard.csv";
        //String path = "data/mllib/creditcardBIG.csv";
        //String path = "data/mllib/kddcup_train.txt";
        //String path = "hdfs://192.168.100.4:9000/spark/kdd_10_proc.txt.gz";


        // load mem data
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSet(path);
        //memDataSet.getDs().cache();//.repartition(4);

        // kmeans test
        KMean kMean = new KMean(sparkSession);
        ClusteringSettings clusteringSettings = new ClusteringSettings()
                .setK(100)
                .setSeed(20L);

        kMean.buildClusterer(memDataSet, clusteringSettings);
        // show predicted clusters
        kMean.getPredictions().show();


        System.out.println(kMean.toString());


        ClusteringEvaluator clusteringEvaluator = new ClusteringEvaluator();
        clusteringEvaluator.setFeaturesCol("features");
        clusteringEvaluator.setPredictionCol("prediction");
        System.out.println("EVAL: "+clusteringEvaluator.evaluate(kMean.getPredictions()));
        ClusteringSummary clusteringSummary = new ClusteringSummary(kMean.getPredictions(), "prediction", "features", kMean.getNoCluster());
        System.out.println(Arrays.toString(clusteringSummary.clusterSizes()));



        //mns.saveAsCSV(kMean.getPredictions());
        //new Scanner(System.in).nextLine();
        sparkSession.close();

    }
}
