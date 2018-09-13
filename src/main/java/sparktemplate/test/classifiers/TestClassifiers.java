package sparktemplate.test.classifiers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import sparktemplate.classifiers.ClassifierSettings;
import sparktemplate.classifiers.Evaluation;
import sparktemplate.datasets.MemDataSet;
import sparktemplate.strings.ClassificationStrings;

/**
 * Created by as on 14.03.2018.
 */
public class TestClassifiers {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("TestClassifiers")
                .setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession spark = new SparkSession(context);

        try {
            String trainPath = "data_test/kdd_train.csv";
            String testPath = "data_test/kdd_test.csv";

            // From csv. (Raw data)
            MemDataSet dataSetTrain = new MemDataSet(spark);
            dataSetTrain.loadDataSetCSV(trainPath);
            MemDataSet dataSetTest = new MemDataSet(spark);
            dataSetTest.loadDataSetCSV(testPath);

            // From parquet. (Prepared data)
            MemDataSet dataSetTrain2 = new MemDataSet(spark);
            dataSetTrain2.loadDataSetPARQUET("data_test/kdd_train_prepared_parquet");
            MemDataSet dataSetTest2 = new MemDataSet(spark);
            dataSetTest2.loadDataSetPARQUET("data_test/kdd_train_prepared_parquet");

            // From JSON. (Prepared data)(Schema required)
            StructType schema = new StructType(new StructField[]{
                    new StructField(ClassificationStrings.featuresCol, new VectorUDT(), false, Metadata.empty()),
                    new StructField(ClassificationStrings.labelCol, DataTypes.StringType, true, Metadata.empty())
            });
            MemDataSet dataSetTest3 = new MemDataSet(spark);
            dataSetTest3.loadDataSetJSON("data_test/kdd_test_prepared_json",schema);

            // Settings.
            ClassifierSettings classifierSettings = new ClassifierSettings();
            classifierSettings
                    .setLabelName("class")
                    .setRandomForest();

            Evaluation evaluation = new Evaluation(spark);
            evaluation.trainAndTest(dataSetTrain2, true, dataSetTest3, true, classifierSettings, false);
            evaluation.printReport();

            // Print metric for decision class.
            System.out.println(evaluation.getMetricByClass("class", "f1"));
            // Show predictions.
            Dataset<Row> predictions = evaluation.getPredictions().select(ClassificationStrings.labelCol, ClassificationStrings.predictedLabelCol, ClassificationStrings.featuresCol);
            predictions.show(10);
            // Print all results.
            //System.out.println("RESULT:\n" + evaluation.getStringBuilder());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

