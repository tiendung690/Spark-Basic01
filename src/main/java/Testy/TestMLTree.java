//package Testy;
//
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.ml.Pipeline;
//import org.apache.spark.ml.PipelineModel;
//import org.apache.spark.ml.PipelineStage;
//import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
//import org.apache.spark.ml.classification.DecisionTreeClassifier;
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
//import org.apache.spark.ml.feature.*;
//import org.apache.spark.sql.*;
//import sparktemplate.dataprepare.DataPrepare;
//import sparktemplate.dataprepare.DataPrepareClassification;
//import sparktemplate.datasets.MemDataSet;
//
///**
// * Created by as on 21.03.2018.
// */
//public class TestMLTree {
//    public static void main(String[] args) {
//
//        // INFO DISABLED
//        Logger.getLogger("org").setLevel(Level.OFF);
//        Logger.getLogger("akka").setLevel(Level.OFF);
//        Logger.getLogger("INFO").setLevel(Level.OFF);
//
//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_JDBC")
//                .set("spark.driver.allowMultipleContexts", "true")
//                .setMaster("local");
//        SparkContext context = new SparkContext(conf);
//        SparkSession spark = new SparkSession(context);
//        // JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
//
//
//        String path = "data/mllib/kdd_short.txt"; //"data/mllib/iris.csv";
//
//        System.out.println("// TEST MemDataSet");
//        MemDataSet memDataSet = new MemDataSet(spark);
//        memDataSet.loadDataSetCSV(path);
//        Dataset<Row> ds = memDataSet.getDs();
//
//
//        // Load the data stored in LIBSVM format as a DataFrame.
//        Dataset<Row> data =  DataPrepareClassification.prepareDataSet(DataPrepare.fillMissingValues(ds)); //spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");
//        data.show();
//
//// Index labels, adding metadata to the label column.
//// Fit on whole dataset to include all labels in index.
//        StringIndexerModel labelIndexer = new StringIndexer()
//                .setInputCol("label")
//                .setOutputCol("indexedLabel")
//                .fit(data);
//
//// Automatically identify categorical features, and index them.
//        VectorIndexerModel featureIndexer = new VectorIndexer()
//                .setInputCol("features")
//                .setOutputCol("indexedFeatures")
//                .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
//                .fit(data);
//
//// Split the data into training and test sets (30% held out for testing).
//        Dataset<Row>[] splits = data.randomSplit(new double[]{0.6, 0.4});
//        Dataset<Row> trainingData = splits[0];
//        Dataset<Row> testData = splits[1];
//
//// Train a DecisionTree model.
//        DecisionTreeClassifier dt = new DecisionTreeClassifier()
//                .setLabelCol("indexedLabel")
//                .setFeaturesCol("indexedFeatures");
//
//// Convert indexed labels back to original labels.
//        IndexToString labelConverter = new IndexToString()
//                .setInputCol("prediction")
//                .setOutputCol("predictedLabel")
//                .setLabels(labelIndexer.labels());
//
//// Chain indexers and tree in a Pipeline.
//        Pipeline pipeline = new Pipeline()
//                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, dt, labelConverter});
//
//// Train model. This also runs the indexers.
//        PipelineModel model = pipeline.fit(trainingData);
//
//// Make predictions.
//        Dataset<Row> predictions = model.transform(testData);
//
//// Select example rows to display.
//      //  predictions.select("predictedLabel", "label", "features").show(10);
//
//        predictions.show();
//
//        DecisionTreeClassificationModel treeModel =
//                (DecisionTreeClassificationModel) (model.stages()[2]);
//        System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());
//
//
//
//// Select (prediction, true label) and compute test error.
//        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
//                .setLabelCol("indexedLabel")
//                .setPredictionCol("prediction")
//                .setMetricName("accuracy");
//        double accuracy = evaluator.evaluate(predictions);
//        System.out.println("Test Error = " + (1.0 - accuracy)+", accuracy:"+accuracy);
//
//        System.out.println("------------------------------------------------------------------");
//
//
//        MulticlassClassificationEvaluator evaluator2 = new MulticlassClassificationEvaluator()
//                .setLabelCol("indexedLabel")
//                .setPredictionCol("prediction")
//                .setMetricName("f1");
//
//        System.out.println("noLABEL : "+evaluator2.getMetricName()+",  ,"+evaluator2.evaluate(predictions));
//
//        evaluator2.setMetricName("weightedPrecision");
//        System.out.println("noLABEL : "+evaluator2.getMetricName()+",  ,"+evaluator2.evaluate(predictions));
//
//        evaluator2.setMetricName("weightedRecall");
//        System.out.println("noLABEL : "+evaluator2.getMetricName()+",  ,"+evaluator2.evaluate(predictions));
//
//        evaluator2.setMetricName("accuracy");
//        System.out.println("noLABEL : "+evaluator2.getMetricName()+",  ,"+evaluator2.evaluate(predictions));
//
//
//
//        System.out.println("------------------------------------------------------------------");
//       // param for metric name in evaluation (supports "f1" (default), "weightedPrecision", "weightedRecall", "accuracy")
//
//        // NEGATIVE
//        System.out.println("SELECT NEGATIVE: ");
//        //predictions.filter(value -> value.getString(0).equals("negative")).show();
//        //predictions.filter(value -> value.get(0).toString().equals("normal")).show();
//
////        Option<Object> index = predictions.schema().getFieldIndex("label");
////        int x = (int) index.get();
////        System.out.println("INDEX INT:"+x);
//
//        int ww = (int) predictions.schema().getFieldIndex("label").get();
//
//
//
//        System.out.println("INDEX: "+ predictions.schema().getFieldIndex("predictedLabel"));
//
//        Dataset<Row> predictions2 = predictions.filter(value -> value.get(ww).toString().equals("negative"));
//       // Dataset<Row> predictions2 = predictions.filter(value -> value.get(0).toString().equals("negative"));
//        predictions2.show();
//
//        MulticlassClassificationEvaluator evaluator3 = new MulticlassClassificationEvaluator()
//                .setLabelCol("indexedLabel")
//                .setPredictionCol("prediction")
//                .setMetricName("f1");
//
//        System.out.println("noLABEL : "+evaluator3.getMetricName()+",  ,"+evaluator3.evaluate(predictions2));
//
//        evaluator3.setMetricName("weightedPrecision");
//        System.out.println("noLABEL : "+evaluator3.getMetricName()+",  ,"+evaluator3.evaluate(predictions2));
//
//        evaluator3.setMetricName("weightedRecall");
//        System.out.println("noLABEL : "+evaluator3.getMetricName()+",  ,"+evaluator3.evaluate(predictions2));
//
//        evaluator3.setMetricName("accuracy");
//        System.out.println("noLABEL : "+evaluator3.getMetricName()+",  ,"+evaluator3.evaluate(predictions2));
//
//
//        System.out.println(evaluator3.toString());
//
////        val labeled = pca.transform(trainingDf).rdd.map(row => LabeledPoint(
////                row.getAs[Double]("label"),
////                row.getAs[org.apache.spark.mllib.linalg.Vector]("pcaFeatures")
////))
//       // Dataset<Row> predictions5 = predictions2.map(value -> new LabeledPoint(value.<Double>getAs(1), value.<VectorUDT>getAs(1)));
//
//        //new LabeledPoint(1.0, value.getAs("indexedFeatures")
//       // System.out.println(predictions2.first().getAs("indexedFeatures").getClass());
//
//
//
//      //  MulticlassMetrics multiclassMetrics = new MulticlassMetrics(predictions2.select("prediction", "indexedFeatures").as(Encoders.DECIMAL()));
//       // System.out.println(multiclassMetrics.confusionMatrix());
//    }
//}
