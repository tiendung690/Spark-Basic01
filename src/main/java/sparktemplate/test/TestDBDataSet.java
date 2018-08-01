package sparktemplate.test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import sparktemplate.datasets.DBDataSet;

/**
 * Created by as on 14.03.2018.
 */
public class TestDBDataSet {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("SparkTemplateTest")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);


//        String url = "jdbc:postgresql://10.2.28.17:5432/postgres";
//        String user = "postgres";
//        String password = "postgres";
//        String table = "dane1";

        // ODCZYT Z BAZY KLASTRA
        String url = "jdbc:postgresql://10.2.28.17:5432/postgres";
        String user = "postgres";
        String password = "postgres";
        String table = "dane2";

        DBDataSet dbDataSet = new DBDataSet(sparkSession, url, user, password, table);
        dbDataSet.connect();
        dbDataSet.getDs().printSchema();
        dbDataSet.getDs().show();
        System.out.println(dbDataSet.getNoRecord());
        System.out.println(dbDataSet.getNoAttr());
        System.out.println(dbDataSet.getAttrName(0));
        dbDataSet.getFirstRecord();
        dbDataSet.getNextRecord();


    }
}
