package com.company.Other;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.*;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// $example on$

/**
 * Created by as on 18.11.2017.
 */
public class SparkRDD {

    // CLASSIFICATION
//    public static JavaPairRDD<Object, Object> decisionTree(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
//
//        System.out.println("DecisionTreeCls----------------------------");
//// Set parameters.
////  Empty categoricalFeaturesInfo indicates all features are continuous.
//        int numClasses = 2;
//        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
//        String impurity = "gini";
//        int maxDepth = 5;
//        int maxBins = 32;
//
//// Train a DecisionTreeCls model for classification.
//        DecisionTreeModel model = DecisionTreeCls.trainClassifier(trainingData, numClasses,
//                categoricalFeaturesInfo, impurity, maxDepth, maxBins);
//
//// Evaluate model on test instances and compute test error
//        JavaPairRDD<Object, Object> predictionAndLabel =
//                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
//        double testErr =
//                predictionAndLabel.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testData.count();
//
//        predictionAndLabel.foreach(data -> {
//            System.out.println("model=" + data._1() + " label=" + data._2());
//        });
//
//
//        //////////////////// only 0
//        JavaPairRDD<Object, Object> predictionAndLabel0 = predictionAndLabel
//                .filter(data -> data._2().equals(0.0));
//
//        predictionAndLabel0.foreach(data -> {
//            System.out.println("0_model=" + data._1() + " 0_label=" + data._2());
//        });
//
//        double testErr0 =
//                predictionAndLabel0.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testData.count();
//
//        System.out.println("Test Error0: " + testErr0);
//        //////////////////// only 1
//        JavaPairRDD<Object, Object> predictionAndLabel1 = predictionAndLabel
//                .filter(data -> data._2().equals(1.0));
//
//        predictionAndLabel1.foreach(data -> {
//            System.out.println("1_model=" + data._1() + " 1_label=" + data._2());
//        });
//
//        double testErr1 =
//                predictionAndLabel1.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testData.count();
//
//        System.out.println("Test Error1: " + testErr1);
//        ///////////////////////////////////////////////////////////////////////////////////////////
//        System.out.println("Test Error ALL: " + testErr);
//        System.out.println("Learned classification tree model:\n" + model.toDebugString());
//
//// Save and load model
////        model.save(jsc.sc(), "target/tmp/myDecisionTreeClassificationModel");
////        System.out.println("load");
////        DecisionTreeModel sameModel = DecisionTreeModel
////                .load(jsc.sc(), "target/tmp/myDecisionTreeClassificationModel");
//
//        return predictionAndLabel;
//    }
//
//    public static JavaPairRDD<Object, Object> randomForest(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
//
//        System.out.println("RandomForestsCls----------------------------");
//// Train a RandomForest model.
//// Empty categoricalFeaturesInfo indicates all features are continuous.
//        Integer numClasses = 2;
//        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
//        Integer numTrees = 3; // Use more in practice.
//        String featureSubsetStrategy = "auto"; // Let the algorithm choose.
//        String impurity = "gini";
//        Integer maxDepth = 5;
//        Integer maxBins = 32;
//        Integer seed = 12345;
//
//        RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses,
//                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
//                seed);
//
//// Evaluate model on test instances and compute test error
//        JavaPairRDD<Object, Object> predictionAndLabel =
//                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
//        double testErr =
//                predictionAndLabel.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testData.count();
//
//        predictionAndLabel.foreach(data -> {
//            System.out.println("model=" + data._1() + " label=" + data._2());
//        });
//
//
//        System.out.println("Test Error: " + testErr);
//        System.out.println("Learned classification forest model:\n" + model.toDebugString());
//
//// Save and load model
////        model.save(jsc.sc(), "target/tmp/myRandomForestClassificationModel");
////        RandomForestModel sameModel = RandomForestModel.load(jsc.sc(),
////                "target/tmp/myRandomForestClassificationModel");
//        return predictionAndLabel;
//    }
//
//    public static JavaPairRDD<Object, Object> naiveBayes(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
//
//        System.out.println("NaiveBayes----------------------------");
//        NaiveBayesModel model = NaiveBayes.train(trainingData.rdd(), 1.0);
//        JavaPairRDD<Object, Object> predictionAndLabel =
//                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
//        double accuracy =
//                predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / (double) testData.count();
//        System.out.println("Test set accuracy = " + accuracy);
//
//        predictionAndLabel.foreach(data -> {
//            System.out.println("model=" + data._1() + " label=" + data._2());
//        });
//
//// Save and load model
////        model.save(jsc.sc(), "target/tmp/myNaiveBayesModel");
////        NaiveBayesModel sameModel = NaiveBayesModel.load(jsc.sc(), "target/tmp/myNaiveBayesModel");
//        return predictionAndLabel;
//    }
//    public static JavaPairRDD<Object, Object> naiveBayes(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
//
//        System.out.println("NaiveBayes----------------------------");
//        NaiveBayesModel model = NaiveBayes.train(trainingData.rdd(), 1.0);
//        JavaPairRDD<Object, Object> predictionAndLabel =
//                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
//        double accuracy =
//                predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / (double) testData.count();
//        System.out.println("Test set accuracy = " + accuracy);
//
//// Save and load model
////        model.save(jsc.sc(), "target/tmp/myNaiveBayesModel");
////        NaiveBayesModel sameModel = NaiveBayesModel.load(jsc.sc(), "target/tmp/myNaiveBayesModel");
//        return predictionAndLabel;
//    }

//    public static JavaPairRDD<Object, Object> linearSVMs(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
//
//        System.out.println("Linear Support Vector Machines----------------------------");
//        // Run training algorithm to build the model.
//        int numIterations = 100;
//        SVMModel model = SVMWithSGD.train(trainingData.rdd(), numIterations);
//
//// Clear the default threshold.
//        model.clearThreshold();
//
////        JavaPairRDD<Object, Object> predictionAndLabel = testData.mapToPair(p ->
////                new Tuple2<>(model.predict(p.features()), p.label()));
//// Compute raw scores on the test set.
//        JavaRDD<Tuple2<Object, Object>> scoreAndLabels = testData.map(p ->
//                new Tuple2<>(model.predict(p.features()), p.label()));
//
//// Save and load model
////        model.save(sc, "target/tmp/javaSVMWithSGDModel");
////        SVMModel sameModel = SVMModel.load(sc, "target/tmp/javaSVMWithSGDModel");
//
//        scoreAndLabels.foreach(data -> {
//            System.out.println("model=" + data._1() + " label=" + data._2());
//        });
//
//        return JavaPairRDD.fromJavaRDD(scoreAndLabels);
//    }

//    public static JavaPairRDD<Object, Object> logisticRegression(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
//
//        System.out.println("Logistic Regression----------------------------");
//
//
//        // Run training algorithm to build the model.
//        LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
//                .setNumClasses(10)
//                .run(trainingData.rdd());
//
//// Compute raw scores on the test set.
//        JavaPairRDD<Object, Object> predictionAndLabels = testData.mapToPair(p ->
//                new Tuple2<>(model.predict(p.features()), p.label()));
//
//
//// Save and load model
////        model.save(sc, "target/tmp/javaLogisticRegressionWithLBFGSModel");
////        LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc,
////                "target/tmp/javaLogisticRegressionWithLBFGSModel");
//
//        predictionAndLabels.foreach(data -> {
//            System.out.println("model=" + data._1() + " label=" + data._2());
//        });
//
//        return predictionAndLabels;
//    }

    // EVALUATION
//    public static void binaryClassification(JavaPairRDD<Object, Object> predictionAndLabels) {
//
//        System.out.println("BINARY CLASSIFICATION-------------------------------------");
//// Get evaluation metrics.
//        BinaryClassificationMetrics metrics =
//                new BinaryClassificationMetrics(predictionAndLabels.rdd());
//
//// Precision by threshold
//        JavaRDD<Tuple2<Object, Object>> precision = metrics.precisionByThreshold().toJavaRDD();
//        System.out.println("Precision by threshold: " + precision.collect());
//
//// Recall by threshold
//        JavaRDD<?> recall = metrics.recallByThreshold().toJavaRDD();
//        System.out.println("Recall by threshold: " + recall.collect());
//
//// F Score by threshold
//        JavaRDD<?> f1Score = metrics.fMeasureByThreshold().toJavaRDD();
//        System.out.println("F1 Score by threshold: " + f1Score.collect());
//
//        JavaRDD<?> f2Score = metrics.fMeasureByThreshold(2.0).toJavaRDD();
//        System.out.println("F2 Score by threshold: " + f2Score.collect());
//
//// Precision-recall curve
//        JavaRDD<?> prc = metrics.pr().toJavaRDD();
//        System.out.println("Precision-recall curve: " + prc.collect());
//
//// Thresholds
//        JavaRDD<Double> thresholds = precision.map(t -> Double.parseDouble(t._1().toString()));
//
//// ROC Curve
//        JavaRDD<?> roc = metrics.roc().toJavaRDD();
//        System.out.println("ROC curve: " + roc.collect());
//
//// AUPRC
//        System.out.println("Area under precision-recall curve = " + metrics.areaUnderPR());
//
//// AUROC
//        System.out.println("Area under ROC = " + metrics.areaUnderROC());
//    }
//
//    public static void multiClassClassification(JavaPairRDD<Object, Object> predictionAndLabels) {
//
//        System.out.println("MultiClassClassification---------------------------");
//        // Get evaluation metrics.
//        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
//
//
////        System.out.println("Accuracy = " + metrics.accuracy());
////        System.out.println("Labels = "+ Arrays.toString(metrics.labels()));
////        System.out.println("wTP = " + metrics.weightedTruePositiveRate());
////        System.out.println("wFP = " + metrics.weightedFalsePositiveRate());
////        System.out.println("wFMeasure = " + metrics.weightedFMeasure());
////        System.out.println("wPrecision = " + metrics.weightedPrecision());
////        System.out.println("wRecall = " + metrics.weightedRecall());
////
////        double[] mm = metrics.labels();
////        for(int i=0; i<mm.length; i++){
////            System.out.println("label"+mm[i]);
////            System.out.println("precison"+metrics.precision(mm[i]));
////            System.out.println("wFMeasure"+metrics.weightedFMeasure(mm[i]));
////            System.out.println("Fmeasure"+metrics.fMeasure(mm[i]));
////            System.out.println("TPrate"+metrics.truePositiveRate(mm[i]));
////            System.out.println("FPrate"+metrics.falsePositiveRate(mm[i]));
////        }
//
//
//        // Confusion matrix
//        Matrix confusion = metrics.confusionMatrix();
//        System.out.println("Confusion matrix: \n" + confusion);
//
//// Overall statistics
//
//// Stats by labels
//        for (int i = 0; i < metrics.labels().length; i++) {
//
//            System.out.format("Class %f precision = %f\n", metrics.labels()[i], metrics.precision(
//                    metrics.labels()[i]));
//            System.out.format("Class %f recall = %f\n", metrics.labels()[i], metrics.recall(
//                    metrics.labels()[i]));
//            System.out.format("Class %f F1 score = %f\n", metrics.labels()[i], metrics.fMeasure(
//                    metrics.labels()[i]));
//
//            System.out.println("##########################");
//        }
//
////Weighted stats
//        System.out.println("Accuracy = " + metrics.accuracy());
//        System.out.format("Weighted precision = %f\n", metrics.weightedPrecision());
//        System.out.format("Weighted recall = %f\n", metrics.weightedRecall());
//        System.out.format("Weighted F1 score = %f\n", metrics.weightedFMeasure());
//        System.out.format("Weighted false positive rate = %f\n", metrics.weightedFalsePositiveRate());
//    }
//
//    public static void regressionModelEvaluation(JavaPairRDD<Object, Object> predictionAndLabels) {
//
//        System.out.println("RegressionModelEvaluation--------------------");
//
//        // Instantiate metrics object
//        RegressionMetrics metrics = new RegressionMetrics(predictionAndLabels.rdd());
//
//        // Squared error
//        System.out.format("MSE = %f\n", metrics.meanSquaredError());
//        System.out.format("RMSE = %f\n", metrics.rootMeanSquaredError());
//
//        // R-squared
//        System.out.format("R Squared = %f\n", metrics.r2());
//
//        // Mean absolute error
//        System.out.format("MAE = %f\n", metrics.meanAbsoluteError());
//
//        // Explained variance
//        System.out.format("Explained Variance = %f\n", metrics.explainedVariance());
//
//    }
//
//    // CLUSTERING K-MEANS
//    public static void clusteringKMeans(JavaSparkContext jsc) {
//
////        1.0 1.0
////        1.5 2.0
////        3.0 4.0
////        5.0 7.0
////        3.5 5.0
////        4.5 5.0
////        3.5 4.5
//
//        System.out.println("K-Means----------------------------");
//
//        // Load and parse data
//        String path = "data/mllib/kmeans_data_NEW.txt";
////        String path = "data/mllib/crime_data.csv";
//        JavaRDD<String> data = jsc.textFile(path);
//        JavaRDD<Vector> parsedData = data.map(s -> {
//            String[] sarray = s.split(" ");
//            double[] values = new double[sarray.length];
//            for (int i = 0; i < sarray.length; i++) {
//                values[i] = Double.parseDouble(sarray[i]);
////                System.out.println("val: " + values[i]);
//            }
//            return Vectors.dense(values);
//        });
//        parsedData.cache();
//
//// Cluster the data into two classes using KMeans
//        int numClusters = 3;
//        int numIterations = 20;
//        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
//
//        System.out.println("Cluster centers:");
//        int i = 0;
//        for (Vector center : clusters.clusterCenters()) {
//            System.out.println("cluster: " + i + " " + center + " predict: " + clusters.predict(center));
//            i++;
//        }
//
//        // wypisanie klastra dla kazdego obiektu
//        parsedData.foreach(rdd -> {
//            System.out.println("For object: " + rdd + " predicted cluster: " + clusters.predict(rdd) + " ");
//        });
//
//
//        double cost = clusters.computeCost(parsedData.rdd());
//        System.out.println("Cost: " + cost);
//// Evaluate clustering by computing Within Set Sum of Squared Errors
//        double WSSSE = clusters.computeCost(parsedData.rdd());
//        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
//
//// Save and load model
////        clusters.save(jsc.sc(), "target/org/apache/spark/JavaKMeansExample/KMeansModel");
////        KMeansModel sameModel = KMeansModel.load(jsc.sc(),
////                "target/org/apache/spark/JavaKMeansExample/KMeansModel");
//
//    }



}
