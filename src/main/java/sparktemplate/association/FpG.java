package sparktemplate.association;

import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import sparktemplate.ASettings;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;

import java.io.IOException;

/**
 * Created by as on 13.03.2018.
 */
public class FpG implements AAssociations {

    private FPGrowthModel fpGrowthModel;
    private Dataset<Row> assocRules;
    public SparkSession sparkSession;

    public FpG(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public void buildAssociations(MemDataSet dataSet, ASettings settings) {
        System.out.println("settings.getValue "+settings.getValue("k1"));
        System.out.println("settings.hasKey "+settings.hasKey("k1"));
        buildAssociations(DataPrepare.prepareDataSet(dataSet.getDs(), sparkSession));
    }

    @Override
    public void buildAssociations(MemDataSet dataSet) {
        buildAssociations(DataPrepare.prepareDataSet(dataSet.getDs(), sparkSession));
    }

    @Override
    public void buildAssociations(DBDataSet dataSet, ASettings settings) {
        buildAssociations(DataPrepare.prepareDataSet(dataSet.getDs(), sparkSession));
    }

    @Override
    public void buildAssociations(DBDataSet dataSet) {
        buildAssociations(DataPrepare.prepareDataSet(dataSet.getDs(), sparkSession));
    }

    @Override
    public void saveAssociationRules(String fileName) throws IOException {
        this.assocRules.write().mode(SaveMode.Overwrite).json(fileName);
        System.out.println("saveAssociationRules: "+fileName);
    }

    @Override
    public void loadAssociationRules(String fileName) throws IOException {
        this.assocRules = sparkSession.read().json(fileName);
        System.out.println("loadAssociationRules: "+fileName);
    }

    private void buildAssociations(Dataset<Row> dataset) {

        FPGrowthModel model = new FPGrowth()
                .setItemsCol("text")
                .setMinSupport(0.01)
                .setMinConfidence(0.4)
                .fit(dataset);

        // Display frequent itemsets.
        model.freqItemsets().show(false);
        // Display generated association rules.
        Dataset<Row> assocRules = model.associationRules();
        assocRules.show(false);
        // transform examines the input items against all the association rules and summarize the
        // consequents as prediction
        model.transform(dataset).show(false);

        this.assocRules = assocRules;
        this.fpGrowthModel = model;
    }


}
