package sparktemplate.testremote;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.dataprepare.DataPrepareAssociations;
import sparktemplate.dataprepare.DataPrepareClassification;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

public class PrepareDataToHDFS {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Prepare_Data_To_HDFS")
                .setMaster("spark://10.2.28.17:7077")
                .setJars(new String[]{"out/artifacts/SparkProject_jar/SparkProject.jar"})
                .set("spark.executor.memory", "15g")
                .set("spark.driver.host", "10.2.28.31");

        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);
        JavaSparkContext jsc = new JavaSparkContext(context);

        String path = "hdfs://10.2.28.17:9000/kdd";
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path);
        Dataset<Row> rawData = memDataSet.getDs();
        rawData.cache();

        // Prepare data for clustering. Save as Parquet.
        conf.log().info("Prepare data for clustering.");
        DataPrepareClustering dataPrepareClustering = new DataPrepareClustering();
        Dataset<Row> preparedClustering = dataPrepareClustering.prepareDataSet(rawData, false, false);
        preparedClustering.show(1, false);
        preparedClustering.printSchema();
        memDataSet.saveDataSetPARQUET("hdfs://10.2.28.17:9000/prepared/kdd_clustering", preparedClustering);

        // Prepare data for assoc rules. Save as Parquet.
        conf.log().info("Prepare data for assoc rules.");
        // Select columns.
        Dataset<Row> selectedColumns = rawData.select("protocol_type", "service", "flag",
                "land", "logged_in", "is_host_login", "is_guest_login", "class");
        Dataset<Row> prepareAssociation = DataPrepareAssociations.prepareDataSet(selectedColumns, sparkSession, false, true);
        prepareAssociation.show(1, false);
        prepareAssociation.printSchema();
        memDataSet.saveDataSetPARQUET("hdfs://10.2.28.17:9000/prepared/kdd_association", prepareAssociation);

        // Prepare data for classification. Save as Parquet.
        conf.log().info("Prepare data for classification.");
        Dataset<Row> prepareClassification = DataPrepareClassification.prepareDataSet(rawData,"class", false);
        prepareClassification.show(1, false);
        prepareClassification.printSchema();
        memDataSet.saveDataSetPARQUET("hdfs://10.2.28.17:9000/prepared/kdd_classification", prepareAssociation);
    }
}
