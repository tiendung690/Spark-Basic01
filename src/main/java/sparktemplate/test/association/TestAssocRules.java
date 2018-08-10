package sparktemplate.test.association;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import sparktemplate.association.AssociationSettings;
import sparktemplate.association.FpG;
import sparktemplate.datasets.MemDataSet;

/**
 * Created by as on 14.03.2018.
 */
public class TestAssocRules {
    public static void main(String[] args) throws Exception {

        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("SparkTemplateTest")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);


        String path = "data_test/groceries.csv";
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path);

        // Settings.
        FpG fpG = new FpG(sparkSession);
        AssociationSettings associationSettings = new AssociationSettings();
        associationSettings.setFPGrowth()
                .setMinSupport(0.01)
                .setMinConfidence(0.01);

        // Build.
        fpG.buildAssociations(memDataSet, associationSettings, false);
        // Save.
        //fpG.saveAssociationRules("data/saved_data/AssocRules");
        // Load.
        //fpG.loadAssociationRules("data/saved_data/AssocRules");

        System.out.println("RESULTS:\n"+fpG.getStringBuilder().toString());

    }
}
