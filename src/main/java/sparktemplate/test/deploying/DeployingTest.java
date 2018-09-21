package sparktemplate.test.deploying;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import sparktemplate.association.AssociationSettings;
import sparktemplate.classifiers.ClassifierSettings;
import sparktemplate.clustering.ClusteringSettings;
import sparktemplate.datasets.MemDataSet;
import sparktemplate.deploying.Deploying;

/**
 * Created by as on 02.08.2018.
 */
public class DeployingTest {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Spark_Experiment")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

        //String path = "data_test/groceries.csv"; // asscoRules
        //String path = "data_test/kdd_train.csv"; // clustering
        String path = "data_test/kdd_train.csv"; // classifiers
        //String path = "data_test/kdd_test.csv"; // classifiers

        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path);

        //System.out.println(Deploying.assocRules(context, memDataSet, associationSettings()));
        System.out.println(Deploying.classification(context, memDataSet, memDataSet,classifierSettings()));
        //System.out.println(Deploying.clustering(context, memDataSet, clusteringSettings()));
       // System.out.println(Deploying.clusteringImpl(context, memDataSet, clusteringSettingsImpl()));


    }

    public static ClusteringSettings clusteringSettings() {
        ClusteringSettings clusteringSettings = new ClusteringSettings();
        clusteringSettings.setKMeans()
                .setK(4)
                .setSeed(15L)
                .setMaxIter(20);
        return clusteringSettings;
    }

    public static ClusteringSettings clusteringSettingsImpl() {
        ClusteringSettings clusteringSettings = new ClusteringSettings();
        clusteringSettings.setKMeansImpl()
                .setK(4)
                .setSeed(15L)
                .setMaxIterations(20);
        return clusteringSettings;
    }

    public static ClassifierSettings classifierSettings() {
        ClassifierSettings classifierSettings = new ClassifierSettings();
        classifierSettings
                .setLabelName("class") //class for kdd
                .setRandomForest();
        return classifierSettings;
    }

    public static AssociationSettings associationSettings() {
        AssociationSettings associationSettings = new AssociationSettings();
        associationSettings.setFPGrowth()
                .setMinSupport(0.01)
                .setMinConfidence(0.4);
        return associationSettings;
    }
}
