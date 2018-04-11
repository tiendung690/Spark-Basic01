package Testy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.stat.Summarizer;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import sparktemplate.datasets.MemDataSet;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by as on 19.03.2018.
 */
public class TestClassifier {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Spark_JDBC")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);
        // JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        String path = "data/mllib/kdd_short.txt"; //"data/mllib/iris.csv";

        System.out.println("// TEST MemDataSet");
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSet(path);
        Dataset<Row> ds = memDataSet.getDs();
        ds.show();
        // ds.describe(ds.columns()).show();


        // without label
        Dataset<Row> noLabel = ds.drop(ds.columns()[ds.columns().length - 1]);

        // only label
        Dataset<Row> dsLabel = ds.drop(noLabel.columns());
        dsLabel.show();


        // find symbolical and numerical
        List<String> listStr = new ArrayList();
        List<String> listNum = new ArrayList();

        for (StructField o : noLabel.schema().fields()) {
            if (o.dataType().equals(DataTypes.StringType)) {
                listStr.add(o.name());
            } else {
                listNum.add(o.name());
            }
        }

        String[] num = listNum.toArray(new String[0]);
        String[] str = listStr.toArray(new String[0]);

        // only numerical without label
        Dataset<Row> dsNum = noLabel.drop(str);

        // only symbolical without label
        Dataset<Row> dsStr = noLabel.drop(num);


        String[] strNoLabel = dsStr.columns();
        String[] numNoLabel = dsNum.columns();


        Dataset<Row> data;


        if (numNoLabel.length + 1 == ds.columns().length) { // if only numerical values in ds

            System.out.println("ONLY NUMERICAL");

            // VECTOR FORM NUmerical original

            VectorAssembler assembler2 = new VectorAssembler()
                    .setInputCols(numNoLabel)
                    .setOutputCol("features");

            Dataset<Row> vectorNum = assembler2.transform(ds).drop(numNoLabel);//.drop(dsNum.columns());
            vectorNum.show();
            data = vectorNum.withColumnRenamed("class", "label");


        } else {

            PipelineStage[] pipelineStages = new PipelineStage[strNoLabel.length];

            for (int i = 0; i < pipelineStages.length; i++) {
                // get current column name
                String currentCol = strNoLabel[i];
                // create indexer on column
                StringIndexer indexer = new StringIndexer()
                        .setInputCol(currentCol)
                        .setOutputCol(currentCol + "*");
                // add indexer to pipeline
                pipelineStages[i] = indexer;
            }

            // set stages to pipeline
            Pipeline pipeline = new Pipeline().setStages(pipelineStages);
            // fit and transform, drop old columns
            PipelineModel pipelineModel = pipeline.fit(ds);

            // wszystko
            Dataset<Row> dsPip = pipelineModel.transform(ds);
            dsPip.show();

            // a1*, a2*, a3* ....
            Dataset<Row> afterIndexer = dsPip.drop(ds.columns());
            afterIndexer.show();


            // ONE-HOT-ENCODER *********************************************
            String[] afterStringIndexer = afterIndexer.columns();
            String[] afterOneHot = new String[afterStringIndexer.length];

            for (int i = 0; i < afterOneHot.length; i++) {
                afterOneHot[i] = new StringBuffer().append(afterStringIndexer[i]).append("*").toString();
            }


            OneHotEncoderEstimator encoderHot = new OneHotEncoderEstimator()
                    .setInputCols(afterStringIndexer)
                    .setOutputCols(afterOneHot);
            /// MODEL
            OneHotEncoderModel oneHotEncoderModel = encoderHot.fit(dsPip);

            Dataset<Row> afterOneHotEncoder = oneHotEncoderModel.transform(dsPip).drop(afterIndexer.columns());
            afterOneHotEncoder.show();

            // VECTOR FORM ONE-HOT

            VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(afterOneHot)
                    .setOutputCol("featuresOHE");


            Dataset<Row> vectorOHE = assembler.transform(afterOneHotEncoder);//.drop(afterOneHotEncoder.columns());
            vectorOHE.show();
            vectorOHE.printSchema();


            if (strNoLabel.length + 1 == ds.columns().length) { // only symbolical

                System.out.println("ONLY SYMBOLICAL");
                // DROP DROP DROP DROP DROP DROP ***************************************
                String[] colsForDelete = afterOneHotEncoder.drop(dsLabel.columns()).columns();

                Dataset<Row> dsFeaturesLabel = vectorOHE.drop(colsForDelete);
                dsFeaturesLabel.show();
                data = dsFeaturesLabel
                        .withColumnRenamed("class", "label")
                        .withColumnRenamed("featuresOHE", "features");

                data.show();

            } else { // MIXED SYMBOLICAL AND NUMERICAL

                System.out.println("MIXED SYMBOLIAL AND NUMERICAL");
                // VECTOR FORM NUmerical original

                VectorAssembler assembler2 = new VectorAssembler()
                        .setInputCols(numNoLabel)
                        .setOutputCol("featuresNUM");

                Dataset<Row> vectorNum = assembler2.transform(vectorOHE);//.drop(dsNum.columns());
                vectorNum.show();

                // Connect vectorOHE and vectorNum

                VectorAssembler assembler3 = new VectorAssembler()
                        .setInputCols(new String[]{"featuresOHE", "featuresNUM"})
                        .setOutputCol("features");

                Dataset<Row> vectorAll = assembler3.transform(vectorNum);//.drop(dsNum.columns());

                // DROP DROP DROP DROP DROP DROP ***************************************

                String[] colsForDelete = vectorNum.drop(dsLabel.columns()).columns();

                Dataset<Row> dsFeaturesLabel = vectorAll.drop(colsForDelete);
                dsFeaturesLabel.show();

                data = dsFeaturesLabel.withColumnRenamed("class", "label");
            }
        }


//        StringIndexer stringIndexer = new StringIndexer();
//        stringIndexer.setInputCol(dsLabel.columns()[0]).setOutputCol("indexedClass");
//
//        Dataset<Row> dsFinal = stringIndexer.fit(dsFeaturesLabel).transform(dsFeaturesLabel);
//        dsFinal.show();


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

// Split the data into training and test sets (30% held out for testing).
        Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

// Train a DecisionTree model.
        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures");

// Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

// Chain indexers and tree in a Pipeline.
        Pipeline pipelineX = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, dt, labelConverter});

// Train model. This also runs the indexers.
        PipelineModel model = pipelineX.fit(trainingData);

// Make predictions.
        Dataset<Row> predictions = model.transform(testData);

// Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(5);

// Select (prediction, true label) and compute test error.
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test Error = " + (1.0 - accuracy));

        DecisionTreeClassificationModel treeModel =
                (DecisionTreeClassificationModel) (model.stages()[2]);
        System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());


    }
}
