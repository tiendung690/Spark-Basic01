package sparktemplate.classifiers;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.ASettings;
import sparktemplate.DataRecord;
import sparktemplate.datasets.ADataSet;

import java.io.IOException;

/**
 * Created by as on 02.08.2018.
 */
public abstract class Classifier implements AClassifier {
    private PipelineModel pipelineModel;
    private SparkSession sparkSession;

    public void setPipelineModel(PipelineModel pipelineModel) {this.pipelineModel = pipelineModel;}

    public void setSparkSession(SparkSession sparkSession) {this.sparkSession = sparkSession;}

    @Override
    public String classify(DataRecord dataRecord, ASettings settings, boolean isPrepared) {
        return ClassifierHelper.classify(dataRecord, settings, this.sparkSession, this.pipelineModel, isPrepared);
    }

    @Override
    public Dataset<Row> classify(ADataSet dbDataSet, ASettings settings, boolean isPrepared) {
        return ClassifierHelper.classify(dbDataSet, settings, this.pipelineModel, isPrepared);
    }

    @Override
    public void saveClassifier(String fileName) throws IOException {
        this.pipelineModel.write().overwrite().save(fileName);
    }

    @Override
    public void loadClassifier(String fileName) throws IOException {
        this.pipelineModel = PipelineModel.read().load(fileName);
    }
}
