package experiments.dataprepare;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.datasets.MemDataSet;

public class CheckStructure {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("CheckStruckt")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                .setMaster("spark://10.2.28.19:7077")
                .setJars(new String[]{"out/artifacts/SparkProject_jar/SparkProject.jar"})
                .set("spark.executor.memory", "15g")
                .set("spark.executor.instances", "1")
                .set("spark.executor.cores", "12")
                //.set("spark.deploy.mode", "cluster")
                //.set("spark.default.parallelism","12")
                //.set("spark.task.cpus", "2")
                .set("spark.driver.host", "10.2.28.34");

        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);
        JavaSparkContext jsc = new JavaSparkContext(context);

//        // Load data from hdfs.
//        String path = "hdfs://10.2.28.17:9000/prepared/kdd_association";
//        MemDataSet memDataSet = new MemDataSet(sparkSession);
//        memDataSet.loadDataSetPARQUET(path);
//        Dataset<Row> rawData = memDataSet.getDs();
//        rawData.show(2,false);
//        rawData.printSchema();


        // Load raw data from hdfs.
        String path = "hdfs://10.2.28.17:9000/kdd";
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path, ",");
        Dataset<Row> rawData = memDataSet.getDs();
        //System.out.println(rawData.storageLevel().replication());
        //rawData.repartition(24);
        System.out.println(rawData.count());
    }
}
