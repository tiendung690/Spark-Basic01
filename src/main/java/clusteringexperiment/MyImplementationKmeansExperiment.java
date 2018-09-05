package clusteringexperiment;

import kmeansimplementation.DataModel;
import kmeansimplementation.KMeansImpl;
import kmeansimplementation.Util;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.ClusteringSummary;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by as on 02.08.2018.
 */
public class MyImplementationKmeansExperiment {
    public static void main(String[] args) {

        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        //Logger.getLogger("INFO").setLevel(Level.OFF);

//        SparkConf conf = new SparkConf()
//                .setAppName("KMeans_Implementation")
//                .set("spark.driver.allowMultipleContexts", "true")
//                .set("spark.eventLog.dir", "file:///C:/logs")
//                .set("spark.eventLog.enabled", "true")
//                .set("spark.driver.memory", "2g")
//                .set("spark.executor.memory", "2g")
//                .setMaster("local[*]");

        SparkConf conf = new SparkConf()
                .setAppName("KMeans_Implementation")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                .setMaster("spark://10.2.28.17:7077")
                .setJars(new String[] { "out/artifacts/SparkProject_jar/SparkProject.jar" })
                //
                .set("spark.executor.memory", "12g")
//                .set("spark.executor.instances", "12")
//                .set("spark.executor.cores", "2")
                //.set("spark.cores.max", "3")
                .set("spark.driver.host", "10.2.28.34");
        SparkContext sc = new SparkContext(conf);
        SparkSession spark = new SparkSession(sc);

        //String path = "data_test/kdd_test.csv";
        //String path = "data/kdd_10_proc.txt";
        //String path = "hdfs://10.2.28.17:9000/kdd/kdd_10_proc.txt";
        String path = "hdfs://10.2.28.17:9000/data/kdd_clustering";

        // Load mem data.
        MemDataSet memDataSet = new MemDataSet(spark);
        //memDataSet.loadDataSetCSV(path,true,true);
        memDataSet.loadDataSetPARQUET(path);

        // Prepare data.
        DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> preparedData = memDataSet.getDs();//dpc.prepareDataSet(memDataSet.getDs(), false, true).select("features"); //normFeatures //features

        // Select initial centers.
        JavaRDD<Row> filteredRDD = preparedData
                .toJavaRDD()
                .zipWithIndex()
                // .filter((Tuple2<Row,Long> v1) -> v1._2 >= start && v1._2 < end)
                .filter((Tuple2<Row, Long> v1) ->
                        //v1._2 == 1 || v1._2 == 200 || v1._2 == 22 || v1._2 == 100 || v1._2 == 300 || v1._2 == 150)
                        v1._2 == 1 || v1._2 == 2 || v1._2 == 22 || v1._2 == 100)
                        //v1._2 == 50 || v1._2 == 2 ||  v1._2 == 100)
                        //v1._2 == 50 || v1._2 == 2)
                .map(r -> r._1);

        // Collect centers from RDD to List.
        ArrayList<Vector> initialCenters = new ArrayList<>();
        initialCenters.addAll(filteredRDD.map(v -> (Vector) v.get(0)).collect());

        // Print first row.
        System.out.println("First row of prepared data:\n" + preparedData.first().get(0));

        // Print centers.
        System.out.println("Initial centers:");
        initialCenters.stream().forEach(t -> System.out.println(t));

        // Convert Dataset to RDD.
        JavaRDD<DataModel> preparedDataRDD = Util.DatasetToRDD(preparedData);

        // Set k.
        int k = initialCenters.size(); // 4;
        // Set max iterations.
        int maxIterations = 5;

        // Random k centers.
        //ArrayList<Vector> initialCenters = initializeCenters(preparedDataRDD, k);

        // Compute final centers.
        ArrayList<Vector> finalCenters = KMeansImpl.computeCenters(preparedDataRDD, initialCenters, 1e-4, maxIterations);

        // Predict clusters.
        JavaPairRDD<Integer, Vector> predictedDataRDD = KMeansImpl.predictCluster(preparedDataRDD, finalCenters);

        // Create Dataset from RDD.
        String featuresCol = "features";
        String predictionCol = "prediction";
        Dataset<Row> predictedData = Util.RDDToDataset(predictedDataRDD, spark, featuresCol, predictionCol);

        // Print predicted data.
        predictedData.printSchema();
        predictedData.show();


        // Print final centers.
        finalCenters.stream().forEach(s -> System.out.println(s));

        // Evaluator for clustering results. The metric computes the Silhouette measure using the squared Euclidean distance.
        ClusteringEvaluator clusteringEvaluator = new ClusteringEvaluator();
        clusteringEvaluator.setFeaturesCol(featuresCol);
        clusteringEvaluator.setPredictionCol(predictionCol);

        // Print evaluation.
        System.out.println("Evaluation (Silhouette measure): " + clusteringEvaluator.evaluate(predictedData));

        // Summary of clustering algorithms.
        ClusteringSummary clusteringSummary = new ClusteringSummary(predictedData, predictionCol, featuresCol, k);

        // Print size of (number of data points in) each testcluster.
        System.out.println(Arrays.toString(clusteringSummary.clusterSizes()));

        // Save results to text file.
        //Util.saveAsCSV(predictedData,featuresCol, predictionCol, "clustering_out/impl_kmeans");

        // Keep job alive, allows access to web ui.
        //new Scanner(System.in).nextLine();

        spark.close();
    }
}
