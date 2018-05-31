package sparktemplate.classifiers;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.DataRecord;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClassification;

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
        Dataset<Row> singleRecordPrepared = DataPrepareClassification.prepareLabeledPoint(singleRecord);

        // make prediction
        Dataset<Row> prediction = pipelineModel.transform(singleRecordPrepared);
        //prediction.show();

        // find predicted label
        String predictedLabel = prediction.select(prediction.col("predictedLabel")).first().toString();
        return predictedLabel;
    }
}
