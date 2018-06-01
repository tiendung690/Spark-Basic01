package Testy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClassification;
import sparktemplate.datasets.MemDataSet;

/**
 * Created by as on 21.03.2018.
 */
public class TestLogisticRegression {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Spark_C")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

        String path = "data/mllib/kdd_short.txt"; //"data/mllib/iris.csv";

        System.out.println("// TEST MemDataSet");
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSet(path);
        Dataset<Row> ds = memDataSet.getDs();
        ds.show();

        Dataset<Row> data = DataPrepareClassification.prepareDataSet(DataPrepare.fillMissingValues(ds));
        data.show();


        // Index labels, adding metadata to the label column.
// Fit on whole dataset to include all labels in index.
        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(data);

// Automatically identify categorical features, and index them.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
                .fit(data);

// Split the data into training and test sets (30% held out for testing).
        Dataset<Row>[] splits = data.randomSplit(new double[]{0.6, 0.4});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

// Train a DecisionTree model.
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures")
                .setFamily("multinomial");

// Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

// Chain indexers and tree in a Pipeline.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, lr, labelConverter});

// Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingData);

// Make predictions.
        Dataset<Row> predictions = model.transform(testData);
        predictions.show();

        System.out.println("SINGLE---SINGLE---SINGLE---SINGLE---SINGLE---SINGLE---SINGLE---SINGLE--- ");
        Dataset<Row> predictionsSingle = model.transform(testData.limit(1));
        predictionsSingle.show();
        System.out.println(predictionsSingle.select(predictionsSingle.col("predictedLabel")).first().toString());




        // Load training data
        //Dataset<Row> training = labelIndexer.transform(dataFrame).drop("label").withColumnRenamed("indexedLabel", "label");

       //training.show();







// Fit the model
        LogisticRegressionModel lrModel = (LogisticRegressionModel) (model.stages()[2]);//lr.fit(training);


// Print the coefficients and intercepts for logistic regression with multinomial family
        System.out.println("Multinomial coefficients: " + lrModel.coefficientMatrix()
                + "\nMultinomial intercepts: " + lrModel.interceptVector());


//
// compute accuracy on the test set
        MulticlassClassificationEvaluator evaluator3 = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
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
