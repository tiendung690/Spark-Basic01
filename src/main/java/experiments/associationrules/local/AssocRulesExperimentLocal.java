package experiments.associationrules.local;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.association.AssociationSettings;
import sparktemplate.association.FpG;
import sparktemplate.dataprepare.DataPrepareAssociations;
import sparktemplate.datasets.MemDataSet;

public class AssocRulesExperimentLocal {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Associations_Local")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                .setMaster("local[2]");

        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);
        JavaSparkContext jsc = new JavaSparkContext(context);

        // Load raw data.
        String path = "data/kddcup_train.txt.gz";
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path);
        // Prepare data with selected columns.
        Dataset<Row> prepared = DataPrepareAssociations.prepareDataSet(memDataSet.getDs()
                        .select("protocol_type", "service", "flag", "land", "logged_in",
                                "is_host_login", "is_guest_login", "class")
                , sparkSession, false, true);
        prepared.repartition(16);
        memDataSet.setDs(prepared);

        // Settings.
        FpG fpG = new FpG(sparkSession);
        AssociationSettings associationSettings = new AssociationSettings();
        associationSettings.setFPGrowth()
                .setMinSupport(0.25)
                .setMinConfidence(0.5);

        // Build.
        fpG.buildAssociations(memDataSet, associationSettings, true);
        // Save.
        //fpG.saveAssociationRules("data/saved_data/AssocRules");
        // Load.
        //fpG.loadAssociationRules("data/saved_data/AssocRules");

        System.out.println("RESULTS:\n" + fpG.getStringBuilder().toString());
    }
}
