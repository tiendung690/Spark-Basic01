package com.company.Classification;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import scala.Serializable;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by as on 29.11.2017.
 */
public class DecisionTreeCls implements Serializable {


    private JavaRDD<LabeledPoint> trainingData;
    private JavaRDD<LabeledPoint> testData;
    private JavaPairRDD<Object, Object> predictionAndLabel;
    private DecisionTreeModel model;

    public DecisionTreeCls(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
        this.trainingData = trainingData;
        this.testData = testData;
    }

    public void setTestData(JavaRDD<LabeledPoint> testData) {
        this.testData = testData;
    }

    public JavaPairRDD<Object, Object> getPredictionAndLabel() {
        return predictionAndLabel;
    }

    public void prediction() {
        System.out.println("DecisionTreeCls------------PREDICTION FOR:" + testData.count() + " OBJECTS----------------");


        // Evaluate model on test instances and compute test error
        JavaPairRDD<Object, Object> predictionAndLabel =
                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
        double testErr =
                predictionAndLabel.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testData.count();

//        predictionAndLabel.foreach(data -> {
//            System.out.println("Predicted model=" + data._1() + " label=" + data._2());
//        });

        System.out.println("Test Error For All: " + testErr);

        this.predictionAndLabel = predictionAndLabel;
    }

    public void trainModel() {

        System.out.println("\nDecisionTreeCls------------MODEL TRAINING----------------");
        // Set parameters.
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
        int numClasses = 25;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        String impurity = "gini";
        int maxDepth = 5;
        int maxBins = 32;

        // Train a DecisionTreeCls model for classification.
        DecisionTreeModel model = org.apache.spark.mllib.tree.DecisionTree.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);
        this.model = model;

        System.out.println("Learned classification tree model:\n" + model.toDebugString());
    }
}
