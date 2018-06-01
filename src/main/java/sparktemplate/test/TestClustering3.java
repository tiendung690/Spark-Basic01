package sparktemplate.test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.clustering.ClusteringSummary;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by as on 23.04.2018.
 */
public class TestClustering3 {
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
                .setMaster("local[*]");

//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Cluster_3x_4c_5g")
//                .setMaster("spark://10.2.28.17:7077")
//                .set("spark.eventLog.dir", "file:///C:/logs")
//                .set("spark.eventLog.enabled", "true")
//                .setJars(new String[]{"out/artifacts/SparkProject_jar/SparkProject.jar"})
//                .set("spark.executor.memory", "5g")
//                //.set("spark.cores.max", "4")
//                //.set("spark.default.parallelism", "12")
//                .set("spark.driver.host", "10.2.28.31");


//        // CONFIG DLA KLASTRA, HISTORY SERVER
//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Cluster_from_submit")
//                //.setMaster("local[*]")
//                .set("spark.eventLog.dir", "file:///tmp/spark-events")
//                .set("spark.eventLog.enabled", "true");


//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Default_Kmeans")
//                .setMaster("spark://192.168.100.4:7077")
//                .setJars(new String[] { "out/artifacts/SparkProject_jar/SparkProject.jar" })
//                //.set("spark.executor.memory", "15g")
//                //.set("spark.default.parallelism", "12")
//                .set("spark.driver.host", "192.168.100.2");

        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);


        //String path = "hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt";
        String path = "data/mllib/kdd_3_proc.txt";
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
        DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> ds1 = dpc.prepareDataSet(memDataSet.getDs(), false, true);

        // KMEANS test
        KMeans kmeans = new KMeans()
                .setK(3)
                .setSeed(20L)
                .setMaxIter(20)
                //.setTol(0)
                //.setInitSteps(1)
                //.setTol(0)
                .setInitMode(org.apache.spark.mllib.clustering.KMeans.RANDOM())
                .setFeaturesCol("features"); //normFeatures

        kmeans.log();


        KMeansModel model = kmeans.fit(ds1);
        System.out.println(Arrays.toString(model.clusterCenters()));
        System.out.println("MAX ITERATIONS " + model.getMaxIter());

        // Make predictions
        Dataset<Row> predictions = model.transform(ds1);

        ClusteringEvaluator clusteringEvaluator = new ClusteringEvaluator();
        clusteringEvaluator.setFeaturesCol("features");
        clusteringEvaluator.setPredictionCol("prediction");
        System.out.println("EVAL: " + clusteringEvaluator.evaluate(predictions));

        ClusteringSummary clusteringSummary = new ClusteringSummary(predictions, "prediction", "features", kmeans.getK());
        System.out.println(Arrays.toString(clusteringSummary.clusterSizes()));
        //mns.saveAsCSV(kMean.getPredictions());
        //new Scanner(System.in).nextLine();
        sparkSession.close();
    }
}
