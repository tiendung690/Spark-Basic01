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
public class TrivialLinearSVM implements AClassifier {

    private PipelineModel pipelineModel;
    private SparkSession sparkSession;

    public TrivialLinearSVM(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public void build(ADataSet dataSet, ASettings settings) {
        this.pipelineModel = buildPipelineModel(dataSet.getDs(), settings);
    }

    @Override
    public String classify(DataRecord dataRecord) {
        return ClassifierHelper.classify(dataRecord, this.sparkSession, this.pipelineModel);
    }

    @Override
    public void saveClassifier(String fileName) throws IOException {
        this.pipelineModel.write().overwrite().save(fileName);
    }

    @Override
    public void loadClassifier(String fileName) throws IOException {
        this.pipelineModel = PipelineModel.read().load(fileName);
    }

    @Override
    public Dataset<Row> classify(ADataSet dbDataSet){
        // prepare data
        Dataset<Row> prepTest = DataPrepareClassification.prepareDataSet(DataPrepare.fillMissingValues(dbDataSet.getDs()));
        // Make predictions
        Dataset<Row> predictions = this.pipelineModel.transform(prepTest);
        //predictions.show(5);
        return predictions;
    }

    private PipelineModel buildPipelineModel(Dataset<Row> trainingData, ASettings settings) {

        Dataset<Row> data = DataPrepareClassification.prepareDataSet(DataPrepare.fillMissingValues(trainingData));
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
