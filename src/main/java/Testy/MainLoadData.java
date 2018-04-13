package Testy;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

/**
 * Created by as on 11.04.2018.
 */
public class MainLoadData {
    public static void main(String[] args) {
        // INFO DISABLED
//        Logger.getLogger("org").setLevel(Level.OFF);
//        Logger.getLogger("akka").setLevel(Level.OFF);
//        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Test_Load_Data")
                .setMaster("spark://10.2.28.17:7077")
                .setJars(new String[]{"out/artifacts/SparkProject_jar/SparkProject.jar"})
                //.set("spark.executor.memory", "15g")
                //.set("spark.executor.cores", "10")
                //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.set("spark.submit.deployMode", "cluster")
                .set("spark.default.parallelism", "12")
                .set("spark.driver.host", "10.2.28.31");

//        SparkConf conf = new SparkConf()
//                .setAppName("SparkTemplateTest_Clustering")
//                .setMaster("local");


        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

        String path = "hdfs://10.2.28.17:9000/spark/kddcup_train.txt.gz";

        // Load training data
        Dataset<Row> ds = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(path); //kdd_10_proc.txt.gz  //"hdfs://10.2.28.17:9000/spark/kmean.txt"

        System.out.println("**********" + context.defaultParallelism() + "  ," + context.defaultMinPartitions());

        //ds.show();
        //ds.printSchema();
        System.out.println(ds.count());



       // System.out.println(ds.count());

        // Convert dataset to JavaRDD of Vectors
//        JavaRDD<Vector> x3 = ds.toJavaRDD().map(row -> (DenseVector) row.get(0));
//        ArrayList<Vector> clusterCenters = new ArrayList<>(x3.takeSample(false, 2));
//        System.out.println(Arrays.toString(clusterCenters.toArray()));

        new Scanner(System.in).nextLine();
        //sparkSession.close();
    }
}
