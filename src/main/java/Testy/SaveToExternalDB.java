package Testy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import sparktemplate.clustering.ClusteringSettings;
import sparktemplate.clustering.KMean;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;

import java.util.Arrays;

/**
 * Created by as on 29.05.2018.
 */
public class SaveToExternalDB {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

//        SparkConf conf = new SparkConf()
//                .setAppName("Default_saveDB_external")
//                .set("spark.driver.allowMultipleContexts", "true")
//                .setMaster("local");

        SparkConf conf = new SparkConf()
                .setAppName("Spark_Default_Kmeans")
                .setMaster("spark://10.2.28.17:7077")
                .setJars(new String[]{"out/artifacts/SparkProject_jar/SparkProject.jar", "local:/root/.ivy2/jars/org.postgresql_postgresql-42.1.1.jar"})
                .set("spark.driver.host", "10.2.28.34");


        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);


        String path = "data/mllib/iris.csv";
        // ODCZYT Z PLIKU
//        MemDataSet memDataSet = new MemDataSet(sparkSession);
//        memDataSet.loadDataSetCSV(path);
//        memDataSet.getDs().printSchema();

        // ODCZYT Z BAZY
        String url = "jdbc:postgresql://10.2.28.17:5432/postgres";
        String user = "postgres";
        String password = "postgres";
        String table = "dane2";

        DBDataSet dbDataSet = new DBDataSet(sparkSession, url, user, password, table);
        dbDataSet.connect();
        //dbDataSet.getDs().show();

        // OBLICZANIE KLASTROW
        KMean kMean = new KMean(sparkSession);
        ClusteringSettings clusteringSettings = new ClusteringSettings();
        clusteringSettings.setKMeans()
                .setK(4)
                .setSeed(10L);
        kMean.buildClusterer(dbDataSet, clusteringSettings, false);
        kMean.getPredictions().printSchema();
        Dataset<Row> dss = kMean.getPredictions();

        StructType schema = new StructType(new StructField[]{
                new StructField("features", DataTypes.StringType, false, Metadata.empty()),
                new StructField("cluster", DataTypes.IntegerType, true, Metadata.empty())
        });
        JavaRDD<Row> ss= dss.toJavaRDD()
                .map(v1 -> {
                    Vector s = (Vector) v1.get(0);
                    String ok = Arrays.toString(s.toArray());
                    return RowFactory.create(ok,v1.get(2));
                });

        Dataset<Row> dm = sparkSession.createDataFrame(ss, schema);
        dm.show();


        // ZAPIS WYNIKOW DO BAZY
        DBDataSet dbDataSet2 = new DBDataSet(sparkSession, url, user, password, "wyniki_irys_kmeans_4k");
        //dbDataSet2.connect();
        dbDataSet2.save(dm);


        ////////////////////
        sparkSession.close();
    }
}
