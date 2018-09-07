package sparktemplate.testremote;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;

public class LocalToDatabase {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("LocalToDatabase")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local[*]");

        SparkContext sparkContext = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(sparkContext);

        String path = "data_test/kdd_test.csv";
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path);
        Dataset<Row> rawData = memDataSet.getDs();

        // Db settings.
        String url = "jdbc:postgresql://10.2.28.17:5432/postgres";
        String user = "postgres";
        String password = "postgres";
        String table = "kdd_test";

        // Connect to DB and save. Will create table if not exist.
        DBDataSet dbDataSet = new DBDataSet(sparkSession, url, user, password, table);
        dbDataSet.save(rawData); // Create and fill table with data.
        //dbDataSet.save(rawData.limit(0)); // Create empty table with columns and types.


    }
}
