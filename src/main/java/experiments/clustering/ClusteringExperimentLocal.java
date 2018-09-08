package experiments.clustering;

import kmeansimplementation.Util;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.clustering.ClusteringSettings;
import sparktemplate.clustering.KMean;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

public class ClusteringExperimentLocal {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Associations_Local")
                //.set("spark.eventLog.dir", "file:///C:/logs")
                //.set("spark.eventLog.enabled", "true")
                .setMaster("local[*]");


        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);
        JavaSparkContext jsc = new JavaSparkContext(context);

        // Load raw data.
        //String path = "data/kddcup_train.txt.gz";
        //String path = "data/kdd_10_proc.txt";
        String path = "data/serce1.csv.gz";
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path,";");
        DataPrepareClustering dataPrepareClustering = new DataPrepareClustering();
        Dataset<Row> prepared = dataPrepareClustering.prepareDataSet(memDataSet.getDs(), false, false);
        prepared.show(1, false);
        prepared.printSchema();
        prepared.repartition(16);
        memDataSet.setDs(prepared);

        // Settings.
        KMean kMean = new KMean(sparkSession);
        ClusteringSettings clusteringSettings = new ClusteringSettings();
        clusteringSettings.setKMeans()
                .setK(4)
                .setSeed(10L)
                .setMaxIter(10);


        kMean.buildClusterer(memDataSet, clusteringSettings, true);
        // Show predicted clusters.
        kMean.getPredictions().show(false);
        // Evaluation.
        kMean.printReport();



        // Take sample,reduce dimensions and save as csv.
        // Take random 10% data.
        Dataset<Row> reducedSize = kMean.getPredictions().sample(0.2);
        // Reduce dimensions to 3.
        Dataset<Row> reduceDim = DataPrepare.reduceDimensions(reducedSize,
                "features","features_reduced",3 ).select("features_reduced","prediction");
        // Coalesce to 1 partition.
        reduceDim.coalesce(1);
        // Save as csv.
        Util.saveAsCSV(reduceDim, "features_reduced", "prediction", "data/kdd2");


    }
}
