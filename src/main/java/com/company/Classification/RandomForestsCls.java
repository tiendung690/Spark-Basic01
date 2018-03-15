package com.company.Classification;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by as on 29.11.2017.
 */
public class RandomForestsCls implements Serializable {

    private JavaRDD<LabeledPoint> trainingData;
    private JavaRDD<LabeledPoint> testData;
    private JavaPairRDD<Object, Object> predictionAndLabel;
    private RandomForestModel model;

    public RandomForestsCls(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
        this.trainingData = trainingData;
        this.testData = testData;
    }

    public void setTestData(JavaRDD<LabeledPoint> testData) {
        this.testData = testData;
    }


    public JavaPairRDD<Object, Object> getPredictionAndLabel() {
        return predictionAndLabel;
    }

    public void prediction(){
        System.out.println("RandomForestsCls-----------PREDICTION FOR:"+testData.count()+" OBJECTS----------------");
        // Evaluate model on test instances and compute test error
        JavaPairRDD<Object, Object> predictionAndLabel =
                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
        double testErr =
                predictionAndLabel.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testData.count();

        System.out.println("Test Error: " + testErr);
        this.predictionAndLabel=predictionAndLabel;
    }


    public void trainModel(){
        System.out.println("\nRandomForestsCls-----------MODEL TRAINING----------------");
        // Train a RandomForest model.
        // Empty categoricalFeaturesInfo indicates all features are continuous.
        Integer numClasses = 25; //2
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        Integer numTrees = 3; // Use more in practice.
        String featureSubsetStrategy = "auto"; // Let the algorithm choose.
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        Integer seed = 12345;

        RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
                seed);

        System.out.println("Learned classification forest model:\n" + model.toDebugString());

        this.model=model;
    }


}
