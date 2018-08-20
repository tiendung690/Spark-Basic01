package sparktemplate.classifiers;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.ASettings;
import sparktemplate.datarecord.DataRecord;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClassification;
import sparktemplate.datasets.ADataSet;
import sparktemplate.strings.ClassificationStrings;

/**
 * Klasa zawierajaca metody pomocne w klasyfikacji.
 * <p>
 * Created by as on 01.06.2018.
 */
public class ClassifierHelper {

    /**
     * Metoda klasyfikujaca pojedynczy obiekt danych.
     *
     * @param dataRecord    obiekt dataRecord
     * @param aSettings     ustawienia
     * @param sparkSession  obiekt SparkSession
     * @param pipelineModel obiekt PipelineModel na podstawie ktorego wyznaczana jest klasa decyzyjna
     * @param isPrepared    dane przygotowane
     * @return klasa decyzyjna
     */
    public static String classify(DataRecord dataRecord, ASettings aSettings, SparkSession sparkSession, PipelineModel pipelineModel, boolean isPrepared, boolean removeStrings) {

        // Create dataset.
        Dataset<Row> singleRecord = DataPrepare.createDataSet(dataRecord.getRow(), dataRecord.getStructType(), sparkSession);
        // Prepare dataset.
        Dataset<Row> singleRecordPrepared;
        if (isPrepared) {
            singleRecordPrepared = singleRecord;
        } else {
            singleRecordPrepared = DataPrepareClassification.prepareDataSet(singleRecord, aSettings.getLabelName(), removeStrings
            );
        }
        // Make prediction.
        Dataset<Row> prediction = pipelineModel.transform(singleRecordPrepared);
        // Find predicted label.
        String predictedLabel = prediction.select(prediction.col(ClassificationStrings.predictedLabelCol)).first().toString();
        return predictedLabel;
    }

    /**
     * Metoda klasyfikujaca zbior danych.
     *
     * @param dbDataSet     dane
     * @param aSettings     ustawienia
     * @param pipelineModel obiekt PipelineModel na podstawie ktorego wyznaczana jest klasa decyzyjna
     * @param isPrepared    dane przygotowane
     * @return dane zaklasyfikowane
     */
    public static Dataset<Row> classify(ADataSet dbDataSet, ASettings aSettings, PipelineModel pipelineModel, boolean isPrepared, boolean removeStrings) {
        // Prepare data.
        Dataset<Row> prepTest;
        if (isPrepared) {
            prepTest = dbDataSet.getDs();
        } else {
            prepTest = DataPrepareClassification.prepareDataSet(DataPrepare.fillMissingValues(dbDataSet.getDs()), aSettings.getLabelName(), removeStrings);
        }
        // Make predictions.
        Dataset<Row> predictions = pipelineModel.transform(prepTest);
        return predictions;
    }

}
