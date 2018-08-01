package sparktemplate.deploying;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import sparktemplate.association.AssociationSettings;
import sparktemplate.classifiers.ClassifierSettings;
import sparktemplate.clustering.ClusteringSettings;
import sparktemplate.datasets.MemDataSet;

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

        //String path = "data/mllib/kdd_3_proc.txt"; // asscoRules
        //String path = "data/mllib/iris.csv"; // clustering
        String path = "data/mllib/iris2.csv"; // classifiers
        //String path = "data/mllib/kdd_5_proc.txt"; // classifiers

        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSet(path);

        //System.out.println(Deploying.assocRules(context, memDataSet, associationSettings()));
        System.out.println(Deploying.classification(context, memDataSet, memDataSet,classifierSettings()));
        //System.out.println(Deploying.clustering(context, memDataSet, clusteringSettings()));


    }

    public static ClusteringSettings clusteringSettings() {
        ClusteringSettings clusteringSettings = new ClusteringSettings();
        clusteringSettings.setKMeans()
                .setK(4)
                .setSeed(10L)
                .setMaxIter(20);
        return clusteringSettings;
    }

    public static ClassifierSettings classifierSettings() {
        ClassifierSettings classifierSettings = new ClassifierSettings();
        classifierSettings
                .setLabelName("species") //class for kdd
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
