package Testy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClassification;
import sparktemplate.datasets.MemDataSet;

/**
 * Created by as on 20.03.2018.
 */
public class TestClassifier2 {
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

        Dataset<Row> dataFrame = DataPrepareClassification.prepareDataSet(DataPrepare.fillMissingValues(ds), "class");
        dataFrame.show();

        String labelColumn = dataFrame.columns()[0];

        StringIndexerModel labelIndexer = new StringIndexer()
                //.setInputCol("label")
                .setInputCol(labelColumn)
                .setOutputCol("indexedLabel")
                .fit(dataFrame);


        Dataset<Row> dataFrame2 = labelIndexer.transform(dataFrame)
                .drop(labelColumn)
                .withColumnRenamed("indexedLabel", "label");
        dataFrame2.show();
        dataFrame2.printSchema();



        Dataset<Row>[] splits = dataFrame2.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];


// create the trainer and set its parameters
        NaiveBayes nb = new NaiveBayes();

// train the model
        NaiveBayesModel model = nb.fit(train);

// Select example rows to display.
        Dataset<Row> predictions = model.transform(test);
        predictions.show();

// compute accuracy on the test set
        MulticlassClassificationEvaluator evaluator3 = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("f1");

        System.out.println("noLABEL : "+evaluator3.getMetricName()+",  ,"+evaluator3.evaluate(predictions));

        evaluator3.setMetricName("weightedPrecision");
        System.out.println("noLABEL : "+evaluator3.getMetricName()+",  ,"+evaluator3.evaluate(predictions));

        evaluator3.setMetricName("weightedRecall");
        System.out.println("noLABEL : "+evaluator3.getMetricName()+",  ,"+evaluator3.evaluate(predictions));

        evaluator3.setMetricName("accuracy");
        System.out.println("noLABEL : "+evaluator3.getMetricName()+",  ,"+evaluator3.evaluate(predictions));


    }
}
