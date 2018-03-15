package com.company.Classification;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import scala.Serializable;
import scala.Tuple2;

/**
 * Created by as on 29.11.2017.
 */
public class LinearSVMCls implements Serializable {

    // LINEAR SVMs ONLY FOR BINARY CLASSIFICATION

    private JavaRDD<LabeledPoint> trainingData;
    private JavaRDD<LabeledPoint> testData;
    private JavaPairRDD<Object, Object> predictionAndLabel;
    private SVMModel model;

    public LinearSVMCls(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
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
        System.out.println("Linear Support Vector Machines------------PREDICTION FOR:"+testData.count()+" OBJECTS----------------");
        // Compute raw scores on the test set.

        JavaRDD<Tuple2<Object, Object>> scoreAndLabels = testData.map(p ->
                new Tuple2<>(model.predict(p.features()), p.label()));

        this.predictionAndLabel=JavaPairRDD.fromJavaRDD(scoreAndLabels);

//        predictionAndLabel.foreach(data -> {
//            System.out.println("Predicted model=" + data._1() + " label=" + data._2());
//        });
    }

    public void trainModel() {
        System.out.println("\nLinear Support Vector Machines------------MODEL TRAINING----------------");
        // Run training algorithm to build the model.
        int numIterations = 100;
        SVMModel model = SVMWithSGD.train(trainingData.rdd(), numIterations);
        // Clear the default threshold.
        model.clearThreshold();
        //        JavaPairRDD<Object, Object> predictionAndLabel = testData.mapToPair(p ->
        //                new Tuple2<>(model.predict(p.features()), p.label()));

        // Save and load model
        //        model.save(sc, "target/tmp/javaSVMWithSGDModel");
        //        SVMModel sameModel = SVMModel.load(sc, "target/tmp/javaSVMWithSGDModel");

        //        scoreAndLabels.foreach(data -> {
        //            System.out.println("model=" + data._1() + " label=" + data._2());
        //        });
        this.model = model;
    }
}
