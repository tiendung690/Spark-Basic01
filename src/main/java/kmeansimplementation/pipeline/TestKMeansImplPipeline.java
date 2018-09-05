package kmeansimplementation.pipeline;

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
public class TestKMeansImplPipeline {
    public static void main(String[] args) {

        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        //Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("KMeans_Implementation")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                .set("spark.driver.memory", "2g")
                .set("spark.executor.memory", "2g")
                .setMaster("local[*]");

        SparkContext sc = new SparkContext(conf);
        SparkSession spark = new SparkSession(sc);

        String path = "data_test/kdd_test.csv";

        // Load mem data.
        MemDataSet memDataSet = new MemDataSet(spark);
        memDataSet.loadDataSetCSV(path);

        // Prepare data.
        DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> preparedData = dpc.prepareDataSet(memDataSet.getDs(), false, true).select("features"); //normFeatures //features

        // Select initial centers.
        JavaRDD<Row> filteredRDD = preparedData
                .toJavaRDD()
                .zipWithIndex()
                // .filter((Tuple2<Row,Long> v1) -> v1._2 >= start && v1._2 < end)
                .filter((Tuple2<Row, Long> v1) ->
                        v1._2 == 1 || v1._2 == 2 || v1._2 == 22 || v1._2 == 100)
                .map(r -> r._1);

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

        // Algorithm settings.
        KMeansImplEstimator kMeansImplEstimator = new KMeansImplEstimator()
                .setFeaturesCol("features")
                .setPredictionCol("prediction")
                .setK(k)
                .setEpsilon(1e-4)
                .setInitialCenters(initialCenters)
                .setMaxIterations(20)
                .setSeed(1L);

        //KMeansImplModel kMeansImplModel = kMeansImplEstimator.fit(preparedData);
        //Dataset<Row> predictions = kMeansImplModel.transform(preparedData);

        // Create pipeline and add stages.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{kMeansImplEstimator});
        // Build model.
        PipelineModel model = pipeline.fit(preparedData);

        // Make predictions.
        Dataset<Row> predictions = model.transform(preparedData);

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


// Add new column.
//Dataset<Row> final12 = otherDataset.select(otherDataset.col("colA"), otherDataSet.col("colB"));
//Dataset<Row> result = final12.withColumn("columnName", lit(1))
