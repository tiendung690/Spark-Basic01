package myimpl2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
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
import java.util.List;

/**
 * Created by as on 18.04.2018.
 */
public class TestKMS {
    public static void main(String[] args) {

        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);


        SparkConf conf = new SparkConf()
                .setAppName("KMeans_Implementation_Euclidean_normalArray")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                //.set("spark.driver.memory", "4g")
                //.set("spark.executor.memory", "4g")
                .setMaster("local[*]");

        SparkContext sc = new SparkContext(conf);
        SparkSession spark = new SparkSession(sc);

        //String path = "hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt.gz";
        //String path = "hdfs://192.168.100.4:9000/spark/kdd_10_proc.txt.gz";
        //String path = "data/mllib/kdd_10_proc.txt.gz";
        String path = "data/mllib/kdd_5_proc.txt";
        //String path = "data/mllib/kddcup_train.txt";
        //String path = "data/mllib/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup.txt";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kmean.txt";
        //String path = "data/mllib/kmean.txt";
        //String path = "data/mllib/iris2.csv";
        //String path = "data/mllib/creditcard.csv";
        //String path = "hdfs:/192.168.100.4/data/mllib/kmean.txt";

        // load mem data
        MemDataSet memDataSet = new MemDataSet(spark);
        memDataSet.loadDataSet(path);

        DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> ds1 = dpc.prepareDataSet(memDataSet.getDs(), false, true);
        ds1.printSchema();
        //Dataset<Row> ds = ds1.select("features");

        //Kms kms = new Kms();
        //KmsModel kmsModel = kms.fit(ds1);
        //kmsModel.transform(memDataSet.getDs()).show();
//        Dataset<Row> dd = kmsModel.transform(ds1);
//        dd.show();
//        dd.printSchema();


        ///////////////////////////////////////////////////////////////////
        JavaRDD<Row> filteredRDD = ds1
                .toJavaRDD()
                .zipWithIndex()
                // .filter((Tuple2<Row,Long> v1) -> v1._2 >= start && v1._2 < end)
                .filter((Tuple2<Row,Long> v1) ->
                        v1._2==1 || v1._2==2 || v1._2==22 || v1._2==100 || v1._2==222)
                .map(r -> r._1);


        List<Vector> cx = filteredRDD.map(v -> (Vector)v.get(0)).collect();

        ArrayList<Vector> newCenters = new ArrayList<>();
        newCenters.addAll(cx);
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        KmsEstimator kmsEstimatorModel = new KmsEstimator()
                .setFeaturesCol("features")
                .setPredictionCol("prediction")
                .setK(5)
               // .setInitialCenters(newCenters)
                //.setMaxIterations(5)
                .setSeed(1L);


        //Chain indexers and tree in a Pipeline.
        //Pipeline pipeline = new Pipeline()
        //        .setStages(new PipelineStage[]{kmsModel});

        // Train model. This also runs the indexers.
        //PipelineModel model = pipeline.fit(ds1);


        // Make predictions.
        Dataset<Row> predictions = kmsEstimatorModel.fit(ds1).transform(ds1);// model.transform(ds1);
        //predictions.show();
        //predictions.printSchema();
        //System.out.println(Arrays.toString(kmsModel.getInitialCenters().toArray()));


        ClusteringEvaluator clusteringEvaluator = new ClusteringEvaluator();
        clusteringEvaluator.setFeaturesCol(kmsEstimatorModel.getFeaturesCol());
        clusteringEvaluator.setPredictionCol(kmsEstimatorModel.getPredictionCol());
        System.out.println("EVAL: " + clusteringEvaluator.evaluate(predictions));

        ClusteringSummary clusteringSummary = new ClusteringSummary(predictions, kmsEstimatorModel.getPredictionCol(), kmsEstimatorModel.getFeaturesCol(), kmsEstimatorModel.getK());
        System.out.println(Arrays.toString(clusteringSummary.clusterSizes()));

        //Util.saveAsCSV(predictions);

    }
}


//     DODAWANIE NOWEJ KOLUMNY

//    Dataset<Row> final12 = otherDataset.select(otherDataset.col("colA"), otherDataSet.col("colB"));
//
//
//    Dataset<Row> result = final12.withColumn("columnName", lit(1))
