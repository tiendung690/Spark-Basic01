package sparktemplate.test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.association.AssociationSettings;
import sparktemplate.association.FpG;
import sparktemplate.datasets.MemDataSet;

import java.io.IOException;

/**
 * Created by as on 14.03.2018.
 */
public class TestAssocRules {
    public static void main(String[] args) throws IOException {

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


        String path = "data/mllib/kdd_3_proc.txt"; //"data/mllib/koszyk.txt"; //"data/mllib/iris.csv";

        System.out.println("// TEST MemDataSet");
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSet(path);
        Dataset<Row> memDs = memDataSet.getDs();
        memDs.show();
        System.out.println("// TEST AssocRules");
        FpG fpG = new FpG(sparkSession);
        AssociationSettings associationSettings = new AssociationSettings();
        associationSettings.setFPGrowth()
                .setMinSupport(0.01)
                .setMinConfidence(0.4);
        // build
        fpG.buildAssociations(memDataSet, associationSettings);
        // save
        //fpG.saveAssociationRules("data/saved_data/AssocRules");
        // load
        //fpG.loadAssociationRules("data/saved_data/AssocRules");



    }
}
