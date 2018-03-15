package sparktemplate.test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.DataRecord;
import sparktemplate.clustering.KMean;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by as on 06.03.2018.
 */
public class SparkTemplateTest {
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


        String url = "jdbc:mysql://localhost:3306/rsds_data";
        String user = "root";
        String password = "";
        String table = "slowa_kluczowe";
        String path = "data/mllib/kdd_10_proc.txt";

        System.out.println("// TEST DBDataSet");
        DBDataSet dbDataSet = new DBDataSet(sparkSession, url, user, password, table);
        dbDataSet.connect();
        dbDataSet.ds.show();
        System.out.println(dbDataSet.ds.count());

        System.out.println("dbDataSet.getAttrName(1) "+dbDataSet.getAttrName(1));
        System.out.println("dbDataSet.getNoAttr() "+dbDataSet.getNoAttr());
        System.out.println("dbDataSet.getNoRecord() "+dbDataSet.getNoRecord());

        System.out.println("// TEST DataRecord");
        DataRecord dataRecord = dbDataSet.getFirstRecord();
        DataRecord dataRecord2 = dbDataSet.getNextRecord();
        DataRecord dataRecord3 = dbDataSet.getNextRecord();

        System.out.println("dataRecord.getAttributeValue(1) "+dataRecord.getAttributeValue(1));
        System.out.println("dataRecord.getAttrType(1) "+dataRecord.getAttrType(1));
        System.out.println("dataRecord.getNoAttr() "+dataRecord.getNoAttr());
        dataRecord.setAttributeValue(1,"hello");
        System.out.println("dataRecord.getAttributeValue(1) "+dataRecord.getAttributeValue(1));
        System.out.println("dataRecord2.getAttributeValue(1) "+dataRecord2.getAttributeValue(1));
        System.out.println("dataRecord3.getAttributeValue(1) "+dataRecord3.getAttributeValue(1));

        System.out.println("// TEST MemDataSet");
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSet(path);
        System.out.println("memDataSet.getAttrName(1) "+memDataSet.getAttrName(1));
        System.out.println("memDataSet.getNoAttr() "+memDataSet.getNoAttr());
        System.out.println("memDataSet.getNoRecord() "+memDataSet.getNoRecord());

        DataRecord dataRecord4 = memDataSet.getDataRecord(1);
        System.out.println("dataRecord4.getAttributeValue(1) "+dataRecord4.getAttributeValue(2));
        System.out.println(Arrays.toString(dataRecord4.getRow().schema().fieldNames()));
        DataRecord dataRecord5 = memDataSet.getDataRecord(7721); //finger
        System.out.println("dataRecord5.getAttributeValue(1) "+dataRecord5.getAttributeValue(2));
        System.out.println(Arrays.toString(dataRecord5.getRow().schema().fieldNames()));


        System.out.println("-----------------CREATE DATASET<ROW> SINGLE ROW------------------------");
        List<Row> rows = new ArrayList<>();
        rows.add(dataRecord.getRow());
        Dataset<Row> df2 = sparkSession.createDataFrame(rows, dataRecord.getStructType());
        df2.printSchema();
        df2.show();

        // KMeans TEST
        KMean kMean = new KMean(sparkSession);
        kMean.buildClusterer(memDataSet);
        System.out.println("clusterRecord: "+kMean.clusterRecord(dataRecord4)+"\ngetNoCluster: "+kMean.getNoCluster());
        System.out.println(kMean.getCluster(1).checkRecord(dataRecord4));
        System.out.println(kMean.getCluster(1).checkRecord(dataRecord5));



        // CLOSE CONNECTION
        try {
            dbDataSet.st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
