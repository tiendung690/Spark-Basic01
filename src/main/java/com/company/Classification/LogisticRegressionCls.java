package com.company.Classification;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

/**
 * Created by as on 29.11.2017.
 */
public class LogisticRegressionCls implements scala.Serializable{

    private JavaRDD<LabeledPoint> trainingData;
    private JavaRDD<LabeledPoint> testData;
    private JavaPairRDD<Object, Object> predictionAndLabel;
    private LogisticRegressionModel model;

    public LogisticRegressionCls(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData) {
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
        System.out.println("Logistic Regression------------PREDICTION FOR:"+testData.count()+" OBJECTS----------------");
        // Compute raw scores on the test set.
        JavaPairRDD<Object, Object> predictionAndLabel = testData.mapToPair(p ->
                new Tuple2<>(model.predict(p.features()), p.label()));
        this.predictionAndLabel=predictionAndLabel;
    }
    public void trainModel(){
        System.out.println("\nLogistic Regression------------MODEL TRAINING----------------");

        // Run training algorithm to build the model.
        LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(10)
                .run(trainingData.rdd());

        this.model=model;
    }
}
