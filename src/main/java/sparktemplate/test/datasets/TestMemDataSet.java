package sparktemplate.test.datasets;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import sparktemplate.datasets.MemDataSet;

/**
 * Created by as on 14.03.2018.
 */
public class TestMemDataSet {
    public static void main(String[] args) {

        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("SparkTemplateTest_Clustering")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");

        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

        String path = "data/mllib/kdd_short2.txt";

        // load mem data
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path);
    }
}
