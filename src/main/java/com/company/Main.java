package com.company;

import com.company.Classification.*;
import com.company.Clustering.KMeansClustering;
import com.company.Conversion.Converter;
import com.company.Evaluation.Evaluation;
import com.company.FrequentPatternMining.FPGrowthMining;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

public class Main {

    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // (null chmod 0644) fix save model error
        //System.setProperty("hadoop.home.dir", "C:\\hadoop");

        // SPARK CONF & CONTEXT
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("JavaDecisionTreeClassificationExample");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);


        // Wczytanie i konwersja danych KDDCUP w formacie CSV

        String headers_path = "data/mllib/kdd_headers.txt"; // ścieżka do pliku z nagłówkami
        String data_path = "data/mllib/kdd_10_proc.txt"; // ścieżka do pliku z danymi
        JavaRDD<LabeledPoint> data = Converter.convert(headers_path, data_path);

        // Wyczytanie danych przykładowych w formacie libsvm

//        String datapath = "data/mllib/sample_libsvm_data.txt";
//        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), datapath).toJavaRDD();

        // Podział danych na treningowe i testowe
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

        //Wczytanie danych do testowanie (pojedynczy obiekt).
        //String datapath2 = "data/mllib/sample_libsvm_single.txt";
        //JavaRDD<LabeledPoint> single_data = MLUtils.loadLibSVMFile(jsc.sc(), datapath2).toJavaRDD();


        // DECISION_TREE
        DecisionTreeCls decisionTree = new DecisionTreeCls(trainingData,testData);
        decisionTree.trainModel();
        // Możliwość ustawienia danych do testowania.
        //decisionTree.setTestData(single_data);
        decisionTree.prediction();
        Evaluation.multiClassClassificationForEachClass(decisionTree.getPredictionAndLabel());

        // RANDOM_FORESTS
        RandomForestsCls randomForests = new RandomForestsCls(trainingData,testData);
        randomForests.trainModel();
        randomForests.prediction();
        Evaluation.multiClassClassificationForEachClass(randomForests.getPredictionAndLabel());

        // NAIVE_BAYES
        NaiveBayesCls naiveBayesCls = new NaiveBayesCls(trainingData, testData);
        naiveBayesCls.trainModel();
        naiveBayesCls.prediction();
        Evaluation.multiClassClassificationForEachClass(naiveBayesCls.getPredictionAndLabel());

        // LOGISTIC_REGRESSION
        LogisticRegressionCls logisticRegressionCls = new LogisticRegressionCls(trainingData, testData);
        logisticRegressionCls.trainModel();
        logisticRegressionCls.prediction();
        Evaluation.multiClassClassificationForEachClass(naiveBayesCls.getPredictionAndLabel());

        // LINEAR SUPPORT VECTOR MACHINES (SVMs)
        LinearSVMCls linearSVM = new LinearSVMCls(trainingData,testData);
        linearSVM.trainModel();
        linearSVM.prediction();
        // Linear SVMs supports only binary classification
        Evaluation.binaryClassification(linearSVM.getPredictionAndLabel());
        // For single object
        //linearSVM.setTestData(single_data);
        linearSVM.prediction();
        Evaluation.binaryClassification(linearSVM.getPredictionAndLabel());


        // KMEANS CLUSTERING
        int numClusters = 3;
        KMeansClustering.clusteringKMeans(jsc, "data/mllib/kmeans_data_NEW.txt", numClusters);

        // FREQUENT PATTERN MINING (FP-GROWTH)
        double minSupport = 0.02;
        double minConfidence = 0.3;
        FPGrowthMining.fpGrowth(jsc, "data/mllib/groceries.csv", minSupport, minConfidence);

    }

}
