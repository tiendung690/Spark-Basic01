package experiments.classification;

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

public class ClassifiersExperiment {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("DecisionTree_12C_15GB_Rezygnacje")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                .setMaster("spark://10.2.28.17:7077")
                .setJars(new String[]{"out/artifacts/SparkProject_jar/SparkProject.jar"})
                .set("spark.executor.memory", "15g")
                .set("spark.executor.instances", "1")
                .set("spark.executor.cores", "12")
                //.set("spark.cores.max", "12")
                .set("spark.driver.host", "10.2.28.34");

        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);
        JavaSparkContext jsc = new JavaSparkContext(context);

        // Compute optimal partitions.
        int executroInstances = Integer.valueOf(conf.get("spark.executor.instances"));
        int executorCores = Integer.valueOf(conf.get("spark.executor.cores"));
        int optimalPartitions = executroInstances * executorCores * 4;

        // Load raw data from hdfs.
        // Training data.
        String path = "hdfs://10.2.28.17:9000/prepared/kdd_classification";
        MemDataSet data = new MemDataSet(sparkSession);
        data.loadDataSetPARQUET(path);
        data.getDs().repartition(optimalPartitions);
        // Testing data. 10% of training data.
        Dataset<Row>[] split = data.getDs().randomSplit(new double[]{0.9,0.1});
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
