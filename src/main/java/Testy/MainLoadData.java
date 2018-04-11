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
                .setAppName("SparkTemplateTest_Clustering")
                .setMaster("spark://10.2.28.17:7077")// .setMaster("spark://192.168.100.4:7077")   ///.setMaster("spark://192.168.56.1:7077")
                .setJars(new String[] { "out/artifacts/SparkProject_jar/SparkProject.jar" })
                .set("spark.executor.memory", "15g")
                .set("spark.submit.deployMode", "cluster")
                .set("spark.driver.host", "10.2.28.31");//.set("spark.driver.host", "192.168.100.2");

//        SparkConf conf = new SparkConf()
//                .setAppName("SparkTemplateTest_Clustering")
//                .setMaster("local");


        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

        // Load training data
        Dataset<Row> ds = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt.gz"); //kdd_10_proc.txt.gz  //"hdfs://10.2.28.17:9000/spark/kmean.txt"

        //ds.show();
        //ds.printSchema();
        System.out.println(ds.count());





       // System.out.println(ds.count());

        // Convert dataset to JavaRDD of Vectors
//        JavaRDD<Vector> x3 = ds.toJavaRDD().map(row -> (DenseVector) row.get(0));
//        ArrayList<Vector> clusterCenters = new ArrayList<>(x3.takeSample(false, 2));
//        System.out.println(Arrays.toString(clusterCenters.toArray()));


        sparkSession.close();
    }
}
