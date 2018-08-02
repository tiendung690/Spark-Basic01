package sparktemplate.classifiers;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.ASettings;
import sparktemplate.DataRecord;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClassification;
import sparktemplate.datasets.ADataSet;

import java.io.IOException;

/**
 * Created by as on 21.03.2018.
 */
public class TrivialLinearSVM extends Classifier {


    public TrivialLinearSVM(SparkSession sparkSession) {
        super.setSparkSession(sparkSession);
    }

    @Override
    public void build(ADataSet dataSet, ASettings settings, boolean isPrepared) {
        super.setPipelineModel(buildPipelineModel(dataSet.getDs(), settings, isPrepared));
    }

    private PipelineModel buildPipelineModel(Dataset<Row> trainingData, ASettings settings, boolean isPrepared) {

        Dataset<Row> data;
        if (isPrepared) {
            data = trainingData;
        } else {
            data = DataPrepareClassification.prepareDataSet(DataPrepare.fillMissingValues(trainingData), settings.getLabelName());
        }
        //data.show();

        // Index labels, adding metadata to the label column.
        // Fit on whole dataset to include all labels in index.
        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(data);

        // Automatically identify categorical features, and index them.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
                .fit(data);

        // Classification
        LinearSVC lsvc1 = (LinearSVC) settings.getModel();

        LinearSVC lsvc = lsvc1
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures")
                .setMaxIter(10)
                .setRegParam(0.1);

        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        // Chain indexers and tree in a Pipeline.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, lsvc, labelConverter});

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(data);
        return model;
    }
}
