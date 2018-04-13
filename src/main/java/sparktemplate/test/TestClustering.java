package sparktemplate.test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
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
public class TestClustering {
    public static void main(String[] args) throws IOException {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

//        SparkConf conf = new SparkConf()
//                .setAppName("SparkTemplateTest_Clustering")
//                .set("spark.driver.allowMultipleContexts", "true")
//                .setMaster("local");

        SparkConf conf = new SparkConf()
                .setAppName("Spark_Default_Kmeans")
                .setMaster("spark://10.2.28.17:7077")
                .setJars(new String[] { "out/artifacts/SparkProject_jar/SparkProject.jar" })
                //.set("spark.executor.memory", "15g")
                .set("spark.default.parallelism", "12")
                .set("spark.driver.host", "10.2.28.31");


        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);


        String path = "hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt.gz";
        //String path = "data/mllib/kdd_10_proc.txt.gz";
        //String path = "data/mllib/iris.csv";

        // load mem data
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSet(path);
        // get single record at index
        DataRecord dataRecord4 = memDataSet.getDataRecord(0);

        // kmeans test
        KMean kMean = new KMean(sparkSession);
        ClusteringSettings clusteringSettings = new ClusteringSettings()
                .setK(2)
                .setSeed(10L);

        kMean.buildClusterer(memDataSet, clusteringSettings);
        // show predicted clusters
        kMean.getPredictions().show(false);
        System.out.println("check predicted cluster for record: " + kMean.clusterRecord(dataRecord4));
        System.out.println("get clusters no.: " + kMean.getNoCluster());

        // check if record exists in each cluster
        for (int i = 0; i < kMean.getNoCluster(); i++) {
            System.out.println("record in cluster " + i + " :" + kMean.getCluster(i).checkRecord(dataRecord4));
        }

        System.out.println(kMean.toString());

        // save
        //kMean.saveClusterer("data/saved_data/Clusters");
        // load
        //kMean.loadClusterer("data/saved_data/Clusters");

        new Scanner(System.in).nextLine();

    }
}
