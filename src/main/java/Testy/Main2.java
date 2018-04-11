package Testy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileReader;

/**
 * Created by as on 25.03.2018.
 */
public class Main2 {
    public static void main(String[] args) {
        // INFO DISABLED
//        Logger.getLogger("org").setLevel(Level.OFF);
//        Logger.getLogger("akka").setLevel(Level.OFF);
//        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("SparkTemplateTest_Clustering")
               .setMaster("spark://10.2.28.17:7077")// .setMaster("spark://192.168.100.4:7077")   ///.setMaster("spark://192.168.56.1:7077")
               // .set("spark.submit.deployMode", "cluster");
//                .setMaster("spark://10.2.28.17:7077")

                .set("spark.driver.host", "10.2.28.31");//.set("spark.driver.host", "192.168.100.2");
//                .set("spark.executor.memory", "4g");

//                .set("spark.driver.allowMultipleContexts", "true")
                //.setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

       // context.addFile("data/mllib/sample_libsvm_data.txt");
        //System.out.println("xxxxx: "+SparkFiles.get("sample_libsvm_data.txt"));

        // Load training data
        Dataset<Row> dataFrame =
                sparkSession.read().format("libsvm")//.load("data/mllib/sample_libsvm_data.txt");
                        .load("hdfs://10.2.28.17:9000/spark/sample_libsvm_data.txt");// .load("hdfs://192.168.100.4:9000/spark/linux_libsvm.txt");
// Split the data into train and test
        Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];

// create the trainer and set its parameters
        NaiveBayes nb = new NaiveBayes();

// train the model
        NaiveBayesModel model = nb.fit(train);

// Select example rows to display.
        Dataset<Row> predictions = model.transform(test);
        predictions.show();

// compute accuracy on the test set
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test set accuracy = " + accuracy);


    }
}

