package sparktemplate.deploying;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import sparktemplate.association.AssociationSettings;
import sparktemplate.association.FpG;
import sparktemplate.classifiers.ClassifierSettings;
import sparktemplate.classifiers.Evaluation;
import sparktemplate.clustering.ClusteringSettings;
import sparktemplate.clustering.KMean;
import sparktemplate.datasets.ADataSet;

/**
 * Created by as on 02.08.2018.
 */
public class Deploying {

    public static String assocRules(SparkContext sparkContext, ADataSet dataSet, AssociationSettings associationSettings) {
        SparkSession sparkSession = new SparkSession(sparkContext);
        FpG fpG = new FpG(sparkSession);
        fpG.buildAssociations(dataSet, associationSettings, false);
        return fpG.getStringBuilder().toString();
    }

    public static String clustering(SparkContext sparkContext, ADataSet dataSet, ClusteringSettings clusteringSettings) {
        SparkSession sparkSession = new SparkSession(sparkContext);
        KMean kMean = new KMean(sparkSession);
        kMean.buildClusterer(dataSet, clusteringSettings, false);
        return kMean.getStringBuilder().toString();
    }

    public static String classification(SparkContext sparkContext, ADataSet dataSetTrain, ADataSet dataSetTest, ClassifierSettings classifierSettings) {
        SparkSession sparkSession = new SparkSession(sparkContext);
        Evaluation evaluation = new Evaluation(sparkSession);
        evaluation.trainAndTest(dataSetTrain, false, dataSetTest, false, classifierSettings,true);
        return evaluation.getStringBuilder().toString();
    }
}
