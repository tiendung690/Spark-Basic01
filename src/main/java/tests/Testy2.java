package tests;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.datasets.MemDataSet;

/**
 * Created by as on 18.03.2018.
 */
public class Testy2 {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Spark_JDBC")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);
        // JavaSparkContext javaSparkContext = new JavaSparkContext(conf);


        String path = "data/mllib/kdd_short.txt"; //"data/mllib/iris.csv";

        System.out.println("// TEST MemDataSet");
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path);
        Dataset<Row> ds = memDataSet.getDs();
        ds.show();

        Dataset<Row> ds2 = DataPrepare.fillMissingValues(ds);
        ds2.show();











//        VectorAssembler assembler = new VectorAssembler()
//                .setInputCols(new String[]{"categoryVec1", "categoryVec2"})
//                .setOutputCol("features");
//
//
//        Dataset<Row> output = assembler.transform(encoded);
//        output.show(false);
//        output.printSchema();
//
//
//        VectorIndexer indexer = new VectorIndexer()
//                .setInputCol("features")
//                .setOutputCol("indexed")
//                .setMaxCategories(2);
//        VectorIndexerModel indexerModel = indexer.fit(output);
//
//        Map<Integer, Map<Double, Integer>> categoryMaps = indexerModel.javaCategoryMaps();
//        System.out.print("Chose " + categoryMaps.size() + " categorical features:");
//
//        for (Integer feature : categoryMaps.keySet()) {
//            System.out.print(" " + feature);
//        }
//        System.out.println();
//
//        // Create new column "indexed" with categorical values transformed to indices
//        Dataset<Row> indexedData = indexerModel.transform(output);
//        indexedData.show();



    }
}
