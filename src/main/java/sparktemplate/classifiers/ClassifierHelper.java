package sparktemplate.classifiers;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.DataRecord;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClassification;
import sparktemplate.datasets.ADataSet;

/**
 * Klasa zawierajaca metody pomocne w klasyfikacji.
 *
 * Created by as on 01.06.2018.
 */
public class ClassifierHelper {

    /**
     * Metoda klasyfikujaca pojedynczy obiekt danych.
     *
     * @param dataRecord obiekt dataRecord
     * @param sparkSession obiekt SparkSession
     * @param pipelineModel obiekt PipelineModel na podstawie ktorego wyznaczana jest klasa decyzyjna
     * @return klasa decyzyjna
     */
    public static String classify(DataRecord dataRecord, SparkSession sparkSession, PipelineModel pipelineModel){

        // create dataset
        Dataset<Row> singleRecord = DataPrepare.createDataSet(dataRecord.getRow(), dataRecord.getStructType(), sparkSession);

        // prepare dataset
        Dataset<Row> singleRecordPrepared = DataPrepareClassification.prepareDataSet(singleRecord);

        // make prediction
        Dataset<Row> prediction = pipelineModel.transform(singleRecordPrepared);
        //prediction.show();

        // find predicted label
        String predictedLabel = prediction.select(prediction.col("predictedLabel")).first().toString();
        return predictedLabel;
    }

    /**
     * Metoda klasyfikujaca zbior danych.
     *
     * @param dbDataSet dane
     * @param pipelineModel obiekt PipelineModel na podstawie ktorego wyznaczana jest klasa decyzyjna
     * @return dane zaklasyfikowane
     */
    public static Dataset<Row> classify(ADataSet dbDataSet, PipelineModel pipelineModel) {
        // prepare data
        Dataset<Row> prepTest = DataPrepareClassification.prepareDataSet(DataPrepare.fillMissingValues(dbDataSet.getDs()));
        // Make predictions
        Dataset<Row> predictions = pipelineModel.transform(prepTest);
        //predictions.show(5);
        return predictions;
    }

}
