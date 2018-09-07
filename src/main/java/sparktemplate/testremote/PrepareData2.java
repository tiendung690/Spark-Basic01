package sparktemplate.testremote;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.dataprepare.DataPrepareClassification;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

public class PrepareData2 {
    public static void main(String[] args) {

        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Prepare_Data_To_HDFS_SERCE")
                .setMaster("spark://10.2.28.17:7077")
                .setJars(new String[]{"out/artifacts/SparkProject_jar/SparkProject.jar"})
                .set("spark.executor.memory", "15g")
                .set("spark.driver.host", "10.2.28.31");

        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);
        JavaSparkContext jsc = new JavaSparkContext(context);

        // Load raw data from hdfs.
        String path = "hdfs://10.2.28.17:9000/serce";
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path, ";");
        Dataset<Row> rawData = memDataSet.getDs();
        rawData.cache();


        // Prepare data for clustering. Save as Parquet.
        conf.log().info("Prepare data for clustering.");
        DataPrepareClustering dataPrepareClustering = new DataPrepareClustering();
        Dataset<Row> preparedClustering = dataPrepareClustering.prepareDataSet(rawData, false, false);
        preparedClustering.show(1, false);
        preparedClustering.printSchema();
        preparedClustering.write().parquet("hdfs://10.2.28.17:9000/prepared/serce_clustering");


        // Prepare data for classification. Save as Parquet.
        conf.log().info("Prepare data for classification.");
        Dataset<Row> prepareClassification = DataPrepareClassification.prepareDataSet(rawData,"diagnoza", false);
        prepareClassification.show(1, false);
        prepareClassification.printSchema();
        prepareClassification.write().parquet("hdfs://10.2.28.17:9000/prepared/serce_classification");
    }
}
