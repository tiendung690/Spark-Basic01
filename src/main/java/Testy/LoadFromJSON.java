package Testy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

import java.util.Arrays;

/**
 * Created by as on 16.04.2018.
 */
public class LoadFromJSON {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("LoadFromJSON")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                //.set("spark.driver.memory", "4g")
                //.set("spark.executor.memory", "4g")
                .setMaster("local[*]");

//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Experiment_Implementation_Kmeans")
//                .setMaster("spark://10.2.28.17:7077")
//                .setJars(new String[]{"out/artifacts/SparkProject_jar/SparkProject.jar"})
//                //.set("spark.executor.memory", "15g")
//                //.set("spark.executor.cores", "10")
//                //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                //.set("spark.submit.deployMode", "cluster")
//                //.set("spark.default.parallelism", "24")
//                .set("spark.driver.host", "10.2.28.31");

//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Experiment_Implementation_Kmeans")
//                .setMaster("spark://192.168.100.4:7077")
//                .setJars(new String[] { "out/artifacts/SparkProject_jar/SparkProject.jar" })
//                //.set("spark.executor.memory", "15g")
//                //.set("spark.default.parallelism", "12")
//                .set("spark.driver.host", "192.168.100.2");

        SparkContext sc = new SparkContext(conf);
        SparkSession spark = new SparkSession(sc);

        //String path = "hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt.gz";
        //String path = "hdfs://192.168.100.4:9000/spark/kdd_10_proc.txt.gz";
        String path = "data/mllib/kdd_10_proc.txt.gz";
        //String path = "data/mllib/kddcup_train.txt";
        //String path = "data/mllib/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup.txt";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kmean.txt";
        //String path = "data/mllib/kmean.txt";
        //String path = "data/mllib/iris.csv";
        //String path = "hdfs:/192.168.100.4/data/mllib/kmean.txt";

        // load mem data
        MemDataSet memDataSet = new MemDataSet(spark);
        memDataSet.loadDataSet(path);

        DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> ds = dpc.prepareDataset(memDataSet.getDs(), false, false).select("features");
        ds.show();
        ds.printSchema();

        //Dataset<Row> ds2 = ds.map(value -> RowFactory.create((DenseVector)value.get(0)));
        JavaRDD<Vector> x3 = ds.toJavaRDD().map(row -> (Vector) row.get(0));
        System.out.println(Arrays.toString(x3.first().toArray()));
        System.out.println(Arrays.toString(x3.first().toDense().toArray()));
        System.out.println(Arrays.toString(x3.first().toSparse().toArray()));

        //x3.saveAsTextFile("data/saved_data/Json");

        Dataset<String> x4 = spark.read().textFile("data/saved_data/Json");
        x4.printSchema();
        x4.show(false);





    }
}
