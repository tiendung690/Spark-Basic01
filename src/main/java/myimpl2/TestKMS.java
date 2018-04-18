package myimpl2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.ClusteringSummary;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeansParams;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.util.DefaultParamsReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

import java.util.Arrays;

import static org.apache.spark.sql.functions.lit;

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
                //.set("spark.eventLog.dir", "file:///C:/logs")
                //.set("spark.eventLog.enabled", "true")
                //.set("spark.driver.memory", "4g")
                //.set("spark.executor.memory", "4g")
                .setMaster("local[*]");

        SparkContext sc = new SparkContext(conf);
        SparkSession spark = new SparkSession(sc);

        //String path = "hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt.gz";
        //String path = "hdfs://192.168.100.4:9000/spark/kdd_10_proc.txt.gz";
        //String path = "data/mllib/kdd_10_proc.txt.gz";
        //String path = "data/mllib/kdd_10_proc.txt";
        //String path = "data/mllib/kddcup_train.txt";
        //String path = "data/mllib/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup.txt";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kmean.txt";
        //String path = "data/mllib/kmean.txt";
        String path = "data/mllib/iris.csv";
        //String path = "data/mllib/creditcard.csv";
        //String path = "hdfs:/192.168.100.4/data/mllib/kmean.txt";

        // load mem data
        MemDataSet memDataSet = new MemDataSet(spark);
        memDataSet.loadDataSet(path);

        DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> ds1 = dpc.prepareDataset(memDataSet.getDs(), false, true);
        //Dataset<Row> ds = ds1.select("features");

        //Kms kms = new Kms();
        //KmsModel kmsModel = kms.fit(ds1);
        //kmsModel.transform(memDataSet.getDs()).show();

//        Dataset<Row> dd = kmsModel.transform(ds1);
//        dd.show();
//        dd.printSchema();

//        ClusteringEvaluator clusteringEvaluator = new ClusteringEvaluator();
//        clusteringEvaluator.setFeaturesCol("values");
//        clusteringEvaluator.setPredictionCol("cluster");
//        System.out.println("EVAL: " + clusteringEvaluator.evaluate(dd));
//
//        ClusteringSummary clusteringSummary = new ClusteringSummary(dd, "cluster", "values", 3);
//        System.out.println(Arrays.toString(clusteringSummary.clusterSizes()));


        Kms kmsModel = new Kms();
        //Chain indexers and tree in a Pipeline.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{kmsModel});

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(ds1);

        // Make predictions.
        Dataset<Row> predictions = model.transform(ds1);
        predictions.show();
        predictions.printSchema();














    }
}


//     DODAWANIE NOWEJ KOLUMNY

//    Dataset<Row> final12 = otherDataset.select(otherDataset.col("colA"), otherDataSet.col("colB"));
//
//
//    Dataset<Row> result = final12.withColumn("columnName", lit(1))
