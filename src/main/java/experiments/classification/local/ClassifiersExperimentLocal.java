package experiments.classification.local;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.classifiers.ClassifierSettings;
import sparktemplate.classifiers.Evaluation;
import sparktemplate.dataprepare.DataPrepareClassification;
import sparktemplate.datasets.MemDataSet;

public class ClassifiersExperimentLocal {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Classifiers_Local")
                //.set("spark.eventLog.dir", "file:///C:/logs")
                //.set("spark.eventLog.enabled", "true")
                .setMaster("local[*]");

        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);
        JavaSparkContext jsc = new JavaSparkContext(context);

        // Training data.
        String path = "data/serce1.csv.gz";
        MemDataSet data = new MemDataSet(sparkSession);
        data.loadDataSetCSV(path,";");
        data.getDs().repartition(16);
        // Prepare data.
        Dataset<Row> prepared = DataPrepareClassification.prepareDataSet(data.getDs(),"diagnoza",false);
        prepared.repartition(16);
        // Testing data. 10% of training data.
        Dataset<Row>[] split = prepared.randomSplit(new double[]{0.9,0.1});
        MemDataSet trainData = new MemDataSet().setDs(split[0]);
        MemDataSet testData = new MemDataSet().setDs(split[1]);

        // Settings.
        ClassifierSettings classifierSettings = new ClassifierSettings();
        classifierSettings.setDecisionTree();

        // Evaluation.
        Evaluation evaluation = new Evaluation(sparkSession);
        evaluation.trainAndTest(trainData, true,
                testData, true,
                classifierSettings, false);
        evaluation.printReport();
        evaluation.getPredictions().show();
    }
}
