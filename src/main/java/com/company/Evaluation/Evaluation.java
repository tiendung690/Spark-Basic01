package com.company.Evaluation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import scala.Tuple2;

/**
 * Created by as on 29.11.2017.
 */
public class Evaluation  {

    // EVALUATION
    public static void multiClassClassification(JavaPairRDD<Object, Object> predictionAndLabels) {

        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());

        double[] mm = metrics.labels();
        for(int i=0; i<mm.length; i++){
            System.out.println("MultiClassClassification-------For Class:"+mm[i]+"--------------");
        }

        // Show data
        //        predictionAndLabels.foreach(data -> {
        //            System.out.println("model=" + data._1() + " label=" + data._2());
        //        });

        // Confusion matrix
        Matrix confusion = metrics.confusionMatrix();
        System.out.println("Confusion matrix: \n" + confusion);

        // Overall statistics

        // Stats by labels
        for (int i = 0; i < metrics.labels().length; i++) {

            System.out.format("Class %f precision = %f\n", metrics.labels()[i], metrics.precision(
                    metrics.labels()[i]));
            System.out.format("Class %f recall = %f\n", metrics.labels()[i], metrics.recall(
                    metrics.labels()[i]));
            System.out.format("Class %f F1 score = %f\n", metrics.labels()[i], metrics.fMeasure(
                    metrics.labels()[i]));

            System.out.println("##########################");
        }

        //Weighted stats
        System.out.println("Accuracy = " + metrics.accuracy());
        System.out.format("Weighted precision = %f\n", metrics.weightedPrecision());
        System.out.format("Weighted recall = %f\n", metrics.weightedRecall());
        System.out.format("Weighted F1 score = %f\n", metrics.weightedFMeasure());
        System.out.format("Weighted false positive rate = %f\n", metrics.weightedFalsePositiveRate());
    }

    /*
    Ocena klasyfikacji dla kazdej klasy decyzyjnej
     */
    public static void multiClassClassificationForEachClass(JavaPairRDD<Object, Object> predictionAndLabels){

        // For all
        Evaluation.multiClassClassification(predictionAndLabels);
        // For each class
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
        double[] mm = metrics.labels();
        for (int i = 0; i < mm.length; i++) {
        //            System.out.println("label" + mm[i]);
            int k = i;
            JavaPairRDD<Object, Object> predictionAndLabel0 = predictionAndLabels
                    .filter(dat -> dat._2().equals(mm[k]));
            System.out.println("\n\n");
            Evaluation.multiClassClassification(predictionAndLabel0);
        }

    }

    public static void binaryClassification(JavaPairRDD<Object, Object> predictionAndLabels) {

        System.out.println("BINARY CLASSIFICATION-------------------------------------");
        // Get evaluation metrics.
        BinaryClassificationMetrics metrics =
                new BinaryClassificationMetrics(predictionAndLabels.rdd());

        // Precision by threshold
        JavaRDD<Tuple2<Object, Object>> precision = metrics.precisionByThreshold().toJavaRDD();
        System.out.println("Precision by threshold: " + precision.collect());

        // Recall by threshold

        JavaRDD<?> recall = metrics.recallByThreshold().toJavaRDD();
        System.out.println("Recall by threshold: " + recall.collect());

        // F Score by threshold
        JavaRDD<?> f1Score = metrics.fMeasureByThreshold().toJavaRDD();
        System.out.println("F1 Score by threshold: " + f1Score.collect());

        JavaRDD<?> f2Score = metrics.fMeasureByThreshold(2.0).toJavaRDD();
        System.out.println("F2 Score by threshold: " + f2Score.collect());

        // Precision-recall curve
        JavaRDD<?> prc = metrics.pr().toJavaRDD();
        System.out.println("Precision-recall curve: " + prc.collect());

        // Thresholds
        JavaRDD<Double> thresholds = precision.map(t -> Double.parseDouble(t._1().toString()));

        // ROC Curve
        JavaRDD<?> roc = metrics.roc().toJavaRDD();
        System.out.println("ROC curve: " + roc.collect());

        // AUPRC
        System.out.println("Area under precision-recall curve = " + metrics.areaUnderPR());

        // AUROC
        System.out.println("Area under ROC = " + metrics.areaUnderROC());
    }

    public static void regressionModelEvaluation(JavaPairRDD<Object, Object> predictionAndLabels) {

        System.out.println("RegressionModelEvaluation--------------------");

        // Instantiate metrics object
        RegressionMetrics metrics = new RegressionMetrics(predictionAndLabels.rdd());

        // Squared error
        System.out.format("MSE = %f\n", metrics.meanSquaredError());
        System.out.format("RMSE = %f\n", metrics.rootMeanSquaredError());

        // R-squared
        System.out.format("R Squared = %f\n", metrics.r2());

        // Mean absolute error
        System.out.format("MAE = %f\n", metrics.meanAbsoluteError());

        // Explained variance
        System.out.format("Explained Variance = %f\n", metrics.explainedVariance());

    }


}
