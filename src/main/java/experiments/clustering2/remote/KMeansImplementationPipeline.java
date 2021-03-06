package experiments.clustering2.remote;

import kmeansimplementation.DistanceName;
import kmeansimplementation.pipeline.KMeansImplEstimator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
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
 * Created by as on 18.04.2018.
 */
public class KMeansImplementationPipeline {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        //Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("KMeans_Implementation")
                //.set("spark.eventLog.dir", "file:///C:/logs")
                //.set("spark.eventLog.enabled", "true")
                .setMaster("spark://10.2.28.19:7077")
                .setJars(new String[]{"out/artifacts/SparkProject_jar/SparkProject.jar"})
                .set("spark.executor.memory", "15g")
                .set("spark.executor.instances", "1")
                .set("spark.executor.cores", "12")
                .set("spark.driver.host", "10.2.28.34");

        SparkContext sc = new SparkContext(conf);
        SparkSession spark = new SparkSession(sc);

        // Compute optimal partitions.
        int executorInstances = Integer.valueOf(conf.get("spark.executor.instances"));
        int executorCores = Integer.valueOf(conf.get("spark.executor.cores"));
        int optimalPartitions = executorInstances * executorCores * 4;
        System.out.println("Partitions: " + optimalPartitions);

        //String path = "hdfs://10.2.28.17:9000/prepared/kdd_clustering";
        String path = "hdfs://10.2.28.17:9000/prepared/serce_clustering";
        //String path = "hdfs://10.2.28.17:9000/prepared/rezygnacje_clustering";

        // Load mem prepared data.
        MemDataSet memDataSet = new MemDataSet(spark);
        ///memDataSet.loadDataSetCSV(path,";"); // ";" - serce, rezygnacje, "," - kdd
        memDataSet.loadDataSetPARQUET(path); // Prepared data.

        // Prepare data.
        //DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> preparedData = memDataSet.getDs(); //dpc.prepareDataSet(memDataSet.getDs(), false, true).select("features"); //normFeatures //features
        preparedData.repartition(optimalPartitions);

        // Select initial centers.
        JavaRDD<Row> filteredRDD = preparedData
                .toJavaRDD()
                .zipWithIndex()
                // .filter((Tuple2<Row,Long> v1) -> v1._2 >= start && v1._2 < end)
                .filter((Tuple2<Row, Long> v1) ->
                        //v1._2 == 1 || v1._2 == 200 || v1._2 == 22 || v1._2 == 100 || v1._2 == 300 || v1._2 == 150 || v1._2 == 450 || v1._2 == 500)
                        //v1._2 == 1 || v1._2 == 200 || v1._2 == 22 || v1._2 == 100 || v1._2 == 300 || v1._2 == 150)
                        //v1._2 == 1 || v1._2 == 2 || v1._2 == 22 || v1._2 == 100)
                        //v1._2 == 50 || v1._2 == 2 ||  v1._2 == 100)
                        v1._2 == 50 || v1._2 == 2)
                .map(r -> r._1);

        System.out.println("Count centers: " + filteredRDD.count());
        // Collect centers from RDD to List.
        ArrayList<Vector> initialCenters = new ArrayList<>();
        initialCenters.addAll(filteredRDD.map(v -> (Vector) v.get(0)).collect());

        // Print first row.
        System.out.println("First row of prepared data:\n" + preparedData.first().get(0));

        // Print centers.
        System.out.println("Initial centers:");
        initialCenters.stream().forEach(t -> System.out.println(t));

        // Set k.
        int k = initialCenters.size(); // 4;
        // Set max iterations.
        int maxIterations = 10;

        // Algorithm settings.
        KMeansImplEstimator kMeansImplEstimator = new KMeansImplEstimator()
                .setDistanceName(DistanceName.EUCLIDEAN)
                .setFeaturesCol("features")
                .setPredictionCol("prediction")
                .setK(k)
                .setEpsilon(1e-4)
                .setSeed(5L) // For random centers, if the initial centers are not set.
                .setInitialCenters(initialCenters)
                .setMaxIterations(maxIterations);


        //KMeansImplModel kMeansImplModel = kMeansImplEstimator.fit(preparedData);
        //Dataset<Row> predictions = kMeansImplModel.transform(preparedData);

        // Create pipeline and add stages.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{kMeansImplEstimator});
        // Build model.
        PipelineModel model = pipeline.fit(preparedData);

        // Make predictions.
        Dataset<Row> predictions = model.transform(preparedData);
        predictions.show();

        ClusteringEvaluator clusteringEvaluator = new ClusteringEvaluator();
        clusteringEvaluator.setFeaturesCol(kMeansImplEstimator.getFeaturesCol());
        clusteringEvaluator.setPredictionCol(kMeansImplEstimator.getPredictionCol());
        // Print evaluation.
        System.out.println("Evaluation (Silhouette measure): " + clusteringEvaluator.evaluate(predictions));
        // Summary of clustering algorithms.
        ClusteringSummary clusteringSummary = new ClusteringSummary(predictions, kMeansImplEstimator.getPredictionCol(), kMeansImplEstimator.getFeaturesCol(), kMeansImplEstimator.getK());
        // Print size of (number of data points in) each testcluster.
        System.out.println(Arrays.toString(clusteringSummary.clusterSizes()));
        // Save results to text file.
        //Util.saveAsCSV(predictedData,featuresCol, predictionCol, "clustering_out/impl_kmeans");

    }
}

