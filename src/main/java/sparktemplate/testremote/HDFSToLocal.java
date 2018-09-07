package sparktemplate.testremote;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import sparktemplate.datasets.MemDataSet;

public class HDFSToLocal {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("HDFSToLocal")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");

        SparkContext sparkContext = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(sparkContext);

        String path = "hdfs://10.2.28.17:9000/test";
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetPARQUET(path);
        Dataset ds = memDataSet.getDs().coalesce(1);
        ds.write().csv("data_test/test.csv");
    }
}
