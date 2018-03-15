package com.company.Classification;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import scala.Serializable;
import scala.Tuple2;

/**
 * Created by as on 29.11.2017.
 */
public class NaiveBayesCls implements Serializable {

    private JavaRDD<LabeledPoint> trainingData;
    private JavaRDD<LabeledPoint> testData;
    private JavaPairRDD<Object, Object> predictionAndLabel;
    private NaiveBayesModel model;

    public NaiveBayesCls(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
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
        System.out.println("NaiveBayes------------PREDICTION FOR:"+testData.count()+" OBJECTS----------------");
        JavaPairRDD<Object, Object> predictionAndLabel =
                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
        this.predictionAndLabel=predictionAndLabel;
    }
    public void trainModel(){
        System.out.println("\nNaiveBayes------------MODEL TRAINING----------------");
        NaiveBayesModel model = NaiveBayes.train(trainingData.rdd(), 1.0);
//        double accuracy =
//                predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / (double) testData.count();
//        System.out.println("Test set accuracy = " + accuracy);
        this.model=model;
    }

}
