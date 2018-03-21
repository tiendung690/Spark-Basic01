package sparktemplate.classifiers;

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Option;
import sparktemplate.ASettings;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;

import javax.xml.crypto.Data;


/**
 * Klasa  <tt>Evaluation</tt> opisuje standardowe funkcjonalnosci obiektu
 * sluzacego do testowania klasyfikatorów
 *
 * @author Jan G. Bazan
 * @version 1.0, luty 2018 roku
 */

public class Evaluation {


    private SparkSession sparkSession;
    private Dataset<Row> predictions;
    private MulticlassClassificationEvaluator evaluator;

    public Evaluation(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction");
    }

    /**
     * Metoda zwracajaca accuracy wykonanego wczesniej eksperymentu
     *
     * @return Wartosc accuracy
     */

    double getAccuracy() {
        return this.evaluator.setMetricName("accuracy").evaluate(this.predictions);
    }

    /**
     * Metoda zwracajaca accuracy dla podanej klasy decyzyjnej, przy czym
     * wartosc decyzji podana jest w formie tekstowej
     *
     * @param decValue Klasa decyzyjna w formie tekstowej
     * @return Wartosc accuracy dla podanej klasy decyzyjnej
     */
    public double getAccuracy(String decValue) {
        int labelIndex = (int) predictions.schema().getFieldIndex("label").get();
        Dataset<Row> predictionsSelected = this.predictions.filter(value -> value.get(labelIndex).toString().equals(decValue));
        return this.evaluator.setMetricName("accuracy").evaluate(predictionsSelected);
    }


    /**
     * Metoda zwracajaca coverage wykonanego wczesniej eksperymentu
     *
     * @return Wartosc coverage
     */
    double getCoverage() {
        return this.evaluator.setMetricName("weightedRecall").evaluate(this.predictions);
    }


    /**
     * Metoda zwracajaca coverage dla podanej klasy decyzyjnej, przy czym
     * wartosc decyzji podana jest w formie tekstowej
     *
     * @param decValue Klasa decyzyjna w formie tekstowej
     * @return Wartosc coverage dla podanej klasy decyzyjnej
     */
    double getCoverage(String decValue) {
        int labelIndex = (int) predictions.schema().getFieldIndex("label").get();
        Dataset<Row> predictionsSelected = this.predictions.filter(value -> value.get(labelIndex).toString().equals(decValue));
        return this.evaluator.setMetricName("weightedRecall").evaluate(predictionsSelected);
    }


    /**
     * Cztery warianty metodTraiAndTest dla zbiorów danych
     *
     * @param trainingDataSet - zbior danych treningowych
     * @param testingDataSet  - zbior danych testowych
     * @param settings        - obiekt parametrow
     */

    void makeTrainAndTest(MemDataSet trainingDataSet, MemDataSet testingDataSet, ASettings classifierSettings) {

    }  //Wykonywaniu testu

    void makeTrainAndTest(MemDataSet trainingDataSet, DBDataSet testingDataSet, ASettings classifierSettings) {
    }  //Wykonywaniu testu

    void makeTrainAndTest(DBDataSet trainingDataSet, DBDataSet testingDataSet, ASettings classifierSettings) {
    }  //Wykonywaniu testu

    void makeTrainAndTest(DBDataSet trainingDataSet, MemDataSet testingDataSet, ASettings classifierSettings) {
    }  //Wykonywaniu testu


    /**
     * Metoda wypisuje na ekran tekst opisujacy wyniki ekperymentu
     */
    public void printReport() {
        System.out.println("Wyniki:");
    }


    void trainTest(MemDataSet memDataSet, MemDataSet testingDataSet, ASettings classifierSettings) {

        String classificationType = classifierSettings.getMap().get("type").toString();

        if(classificationType.equals("linearsvm")){

            System.out.println("type: "+classificationType);
            TrivialLinearSVM algo = new TrivialLinearSVM(sparkSession);
            algo.build(memDataSet, classifierSettings);
            this.predictions = algo.makePredictions(testingDataSet);

        }else if (classificationType.equals("decisiontree")){

            System.out.println("type: "+classificationType);
            TrivialDecisionTree algo = new TrivialDecisionTree(sparkSession);
            algo.build(memDataSet, classifierSettings);
            this.predictions = algo.makePredictions(testingDataSet);

        }else if (classificationType.equals("randomforests")){

            System.out.println("type: "+classificationType);
            TrivialRandomForests algo = new TrivialRandomForests(sparkSession);
            algo.build(memDataSet, classifierSettings);
            this.predictions = algo.makePredictions(testingDataSet);

        }else if (classificationType.equals("logisticregression")){

            System.out.println("type: "+classificationType);
            TrivialLogisticRegression algo = new TrivialLogisticRegression(sparkSession);
            algo.build(memDataSet, classifierSettings);
            this.predictions = algo.makePredictions(testingDataSet);

        }else if (classificationType.equals("naivebayes")){

            System.out.println("type: "+classificationType);
            TrivialNaiveBayes algo = new TrivialNaiveBayes(sparkSession);
            algo.build(memDataSet, classifierSettings);
            this.predictions = algo.makePredictions(testingDataSet);

        }else{
            System.out.println("Wrong classification type!");
        }
    }


}
