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
import sparktemplate.clustering.KMean;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by as on 13.03.2018.
 */
public class TestClustering {
    public static void main(String[] args) throws IOException {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("SparkTemplateTest")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);


        String path =  "data/mllib/kdd_10_proc.txt"; //"data/mllib/iris.csv";

        System.out.println("// TEST MemDataSet");
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSet(path);

        DataRecord dataRecord4 = memDataSet.getDataRecord(0);


        // KMeans TEST
        KMean kMean = new KMean(sparkSession);
        kMean.buildClusterer(memDataSet);
        System.out.println("clusterRecord: " + kMean.clusterRecord(dataRecord4) + "\ngetNoCluster: " + kMean.getNoCluster());
        //System.out.println(kMean.getCluster(3).checkRecord(dataRecord4)); // PROBLEM
        for (int i = 0; i < kMean.getNoCluster(); i++) {
            System.out.println(" __ " + kMean.getCluster(i).checkRecord(dataRecord4));
        }

        // save
        //kMean.saveClusterer("data/saved_data/Clusters");
        // load
        //kMean.loadClusterer("data/saved_data/Clusters");

    }
}
