package sparktemplate.test.clustering;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.clustering.ClusteringSummary;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.sql.SparkSession;
import sparktemplate.DataRecord;
import sparktemplate.clustering.ClusteringSettings;
import sparktemplate.clustering.KMean;
import sparktemplate.datasets.MemDataSet;
import sparktemplate.strings.ClusteringStrings;

import java.io.IOException;
import java.util.Arrays;

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
                .setAppName("TestClustering")
                .setMaster("local[*]");


        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

        //String path = "hdfs://10.2.28.17:9000/user/kdd_10_proc.txt";
        String path = "data_test/kdd_train.csv";

        // Load data.
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path);
        // Get single record at index.
        DataRecord dataRecord4 = memDataSet.getDataRecord(0);

        // Settings.
        KMean kMean = new KMean(sparkSession);
        ClusteringSettings clusteringSettings = new ClusteringSettings();
        clusteringSettings.setKMeans()
                .setK(4)
                .setSeed(10L)
                .setMaxIter(20);


        kMean.buildClusterer(memDataSet, clusteringSettings,false);
        // Show predicted clusters.
        kMean.getPredictions().show(false);
        kMean.getPredictions().printSchema();
        System.out.println(Arrays.toString(kMean.getPredictions().schema().fields()));
        // Evaluate.
        ClusteringEvaluator clusteringEvaluator = new ClusteringEvaluator();
        clusteringEvaluator.setFeaturesCol(ClusteringStrings.featuresCol);
        clusteringEvaluator.setPredictionCol(ClusteringStrings.predictionCol);
        System.out.println("EVAL: " + clusteringEvaluator.evaluate(kMean.getPredictions()));
        ClusteringSummary clusteringSummary = new ClusteringSummary(kMean.getPredictions(), ClusteringStrings.predictionCol, ClusteringStrings.featuresCol, kMean.getNoCluster());
        System.out.println(Arrays.toString(clusteringSummary.clusterSizes()));
        // Check cluster for single record.
        System.out.println("check predicted cluster for record: " + kMean.clusterRecord(dataRecord4, false));
        System.out.println("get clusters no.: " + kMean.getNoCluster());

        // check if record exists in each cluster
        for (int i = 0; i < kMean.getNoCluster(); i++) {
            System.out.println("record in cluster " + i + " :" + kMean.getCluster(i).checkRecord(dataRecord4, false));
        }

        System.out.println(kMean.getCenters());
        System.out.println(kMean.getCluster(1).toString());


        //System.out.println(kMean.getStringBuilder());

        // save
        //kMean.saveClusterer("data/saved_data/Clusters");
        // load
        //kMean.loadClusterer("data/saved_data/Clusters");

        //new Scanner(System.in).nextLine();
        sparkSession.close();

    }
}
