package sparktemplate.classifiers;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.ASettings;
import sparktemplate.DataRecord;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClassification;
import sparktemplate.datasets.ADataSet;
import sparktemplate.strings.ClassificationStrings;

import java.io.IOException;

/**
 * Created by as on 21.03.2018.
 */
public class TrivialNaiveBayes extends Classifier {

    public TrivialNaiveBayes(SparkSession sparkSession) {
        super.setSparkSession(sparkSession);
    }

    @Override
    public void build(ADataSet dataSet, ASettings settings, boolean isPrepared, boolean removeStrings) {
        super.setPipelineModel(buildPipelineModel(dataSet.getDs(), settings, isPrepared, removeStrings));
    }

    private PipelineModel buildPipelineModel(Dataset<Row> trainingData, ASettings settings, boolean isPrepared, boolean removeStrings) {

        Dataset<Row> data;
        if (isPrepared) {
            data = trainingData;
        } else {
            data = DataPrepareClassification.prepareDataSet(DataPrepare.fillMissingValues(trainingData), settings.getLabelName(), removeStrings);
        }

        // Classification algorithm.
        NaiveBayes naiveBayes = ((NaiveBayes) settings.getModel())
                .setLabelCol(ClassificationStrings.indexedLabelCol)
                .setFeaturesCol(ClassificationStrings.indexedFeaturesCol)
                .setPredictionCol(ClassificationStrings.predictionCol);

        // Add algorithm to Pipeline.
        PipelineStage[] pipelineStages = PipelineStagesCreator.createPipelineStages(data, naiveBayes);
        Pipeline pipeline = new Pipeline().setStages(pipelineStages);


        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(data);
        return model;
    }
}
