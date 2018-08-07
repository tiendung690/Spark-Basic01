package sparktemplate.test.dataprepare;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.dataprepare.DataPrepareAssociations;
import sparktemplate.datasets.MemDataSet;

/**
 * Created by as on 07.08.2018.
 */
public class TestPrepareDataSetAssociations {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("TestPrepareDataSetAssociations")
                .setMaster("local[*]");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

        String path =  "data_test/basket_associations.csv";
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        // Load data without header.
        memDataSet.loadDataSet(path,false,false);

        // Raw data.
        Dataset<Row> ds = memDataSet.getDs();
        ds.printSchema();
        ds.show();

        // Prepared data.
        Dataset<Row> ds2 = DataPrepareAssociations.prepareDataSet(ds, sparkSession);
        ds2.show(false);
        ds2.printSchema();


    }
}
