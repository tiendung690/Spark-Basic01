package sparktemplate.classifiers;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.ASettings;
import sparktemplate.DataRecord;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClassification;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;

import java.io.IOException;

/**
 * Created by as on 21.03.2018.
 */
public class TrivialDecisionTree implements AClassifier {

    //private Dataset<Row> predictions;
    private PipelineModel pipelineModel;
    private SparkSession sparkSession;

    public TrivialDecisionTree(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public void build(MemDataSet dataSet, ASettings settings) {
        this.pipelineModel = buildPipelineModel(dataSet.getDs());
    }

    @Override
    public void build(DBDataSet dataSet, ASettings settings) {
        this.pipelineModel = buildPipelineModel(dataSet.getDs());
    }

    @Override
    public String classify(DataRecord dataRecord) {

        // create dataset
        Dataset<Row> singleRecord = DataPrepare.createDataSet(dataRecord.getRow(), dataRecord.getStructType(), this.sparkSession);

        // prepare dataset
        Dataset<Row> singleRecordPrepared = DataPrepareClassification.prepareLabeledPoint(singleRecord);

        // make prediction
        Dataset<Row> prediction = this.pipelineModel.transform(singleRecordPrepared);
        //prediction.show();

        // find predicted label
        String predictedLabel = prediction.select(prediction.col("predictedLabel")).first().toString();
        return predictedLabel;
    }

    @Override
    public void saveClassifier(String fileName) throws IOException {
        this.pipelineModel.write().overwrite().save(fileName);
    }

    @Override
    public void loadClassifier(String fileName) throws IOException {
        this.pipelineModel = PipelineModel.read().load(fileName);
    }


    private PipelineModel buildPipelineModel(Dataset<Row> trainingData) {

        Dataset<Row> data = DataPrepareClassification.prepareLabeledPoint(DataPrepare.fillMissingValues(trainingData));
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
        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures");

        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        // Chain indexers and tree in a Pipeline.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, dt, labelConverter});

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(data);
        return model;
    }

    public Dataset<Row> makePredictions(MemDataSet memDataSet) {

        // prepare data
        Dataset<Row> prepTest = DataPrepareClassification.prepareLabeledPoint(DataPrepare.fillMissingValues(memDataSet.getDs()));
        // make predictions
        Dataset<Row> predictions = this.pipelineModel.transform(prepTest);
        //predictions.show(5);
        return predictions;
    }

    public Dataset<Row> makePredictions(DBDataSet dbDataSet) {
        // prepare data
        Dataset<Row> prepTest = DataPrepareClassification.prepareLabeledPoint(DataPrepare.fillMissingValues(dbDataSet.getDs()));
        // Make predictions
        Dataset<Row> predictions = this.pipelineModel.transform(prepTest);
        //predictions.show(5);
        return predictions;
    }


}
