package sparktemplate.test.dataprepare;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

/**
 * Created by as on 07.08.2018.
 */
public class TestPrepareDataSetClustering {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        //Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("TestPrepareDataSetClustering")
                .setMaster("local[*]");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

        String path = "data_test_prepare/data_clustering_mixed.csv";
        //String path = "data_test_prepare/data_clustering_only_numerical.csv";
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path);

        // Raw data.
        Dataset<Row> ds = memDataSet.getDs();
        ds.show();
        ds.printSchema();

        // Prepared data.
        DataPrepareClustering dataPrepareClustering = new DataPrepareClustering();
        Dataset<Row> ds2 = dataPrepareClustering.prepareDataSet(ds, false, false);
        ds2.show(false);
        ds2.printSchema();

        // Prepared new data based on an existing model.
        // dataPrepareClustering.prepareDataSet(DataPrepare.createDataSet(ds.first(), ds.schema(), sparkSession), true, false).show(false);



    }
}
