package Testy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;

/**
 * Created by as on 29.05.2018.
 */
public class SaveToExternalDB {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Default_saveDB_external")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");

        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);


//        String path = "data/mllib/iris2.csv";

//        MemDataSet memDataSet = new MemDataSet(sparkSession);
//        memDataSet.loadDataSet(path);
//
//        memDataSet.getDs().printSchema();

        String url = "jdbc:postgresql://10.2.28.17:5432/postgres";
        String user = "postgres";
        String password = "postgres";
        String table = "dane2";

        DBDataSet dbDataSet = new DBDataSet(sparkSession, url, user, password, table);
        dbDataSet.connect();

        dbDataSet.getDs().show();

    }
}
