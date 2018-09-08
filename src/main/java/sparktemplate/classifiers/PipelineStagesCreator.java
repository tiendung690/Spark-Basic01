package sparktemplate.classifiers;

import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import sparktemplate.strings.ClassificationStrings;

/**
 * Created by as on 08.08.2018.
 */
public class PipelineStagesCreator {
    public static PipelineStage[] createPipelineStages(Dataset<Row> data, PipelineStage pipelineStage){

        // Stage 1.
        // Index labels, adding metadata to the label column.
        // Fit on whole dataset to include all labels in index.
        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol(ClassificationStrings.labelCol)
                .setOutputCol(ClassificationStrings.indexedLabelCol)
                .setHandleInvalid("skip") // Use when testing data don not contains all labels. Fix problem with LinearSVM.
                .fit(data);

        // Stage 2.
        // Automatically identify categorical features, and index them.
        VectorIndexer featureIndexer = new VectorIndexer()
                .setInputCol(ClassificationStrings.featuresCol)
                .setOutputCol(ClassificationStrings.indexedFeaturesCol)
                .setHandleInvalid("keep")  // Use when testing data don not contains all labels.
                .setMaxCategories(10); // features with > 4 distinct values are treated as continuous.

        // Stage 3.
        // Class that extends PipelineStage, e.g. classification algorithms.

        // Stage 4.
        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol(ClassificationStrings.predictionCol)
                .setOutputCol(ClassificationStrings.predictedLabelCol)
                .setLabels(labelIndexer.labels());

        // Set stages.
        PipelineStage[] pipelineStages = new PipelineStage[4];
        pipelineStages[0] = labelIndexer;
        pipelineStages[1] = featureIndexer;
        pipelineStages[2] = pipelineStage;
        pipelineStages[3] = labelConverter;

        return pipelineStages;
    }
}
