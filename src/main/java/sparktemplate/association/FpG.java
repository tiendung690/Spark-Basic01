package sparktemplate.association;

import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import sparktemplate.ASettings;
import sparktemplate.dataprepare.DataPrepareAssociations;
import sparktemplate.datasets.ADataSet;

import java.io.IOException;

/**
 * Created by as on 13.03.2018.
 */
public class FpG implements AAssociations {

    private FPGrowthModel fpGrowthModel;
    private Dataset<Row> assocRules;
    private SparkSession sparkSession;
    private StringBuilder stringBuilder;

    public FpG(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.stringBuilder = new StringBuilder();
    }

    public StringBuilder getStringBuilder() {
        return stringBuilder;
    }

    @Override
    public void buildAssociations(ADataSet dataSet, ASettings settings, boolean isPrepared) {
        if (isPrepared) {
            buildAssociations(dataSet.getDs(), settings);
        } else {
            buildAssociations(DataPrepareAssociations.prepareDataSet(dataSet.getDs(), sparkSession), settings);
        }
    }

    @Override
    public void saveAssociationRules(String fileName) throws IOException {
        this.assocRules.write().mode(SaveMode.Overwrite).json(fileName);
        System.out.println("saveAssociationRules: " + fileName);
    }

    @Override
    public void loadAssociationRules(String fileName) throws IOException {
        this.assocRules = sparkSession.read().json(fileName);
        System.out.println("loadAssociationRules: " + fileName);
    }

    private void buildAssociations(Dataset<Row> dataset, ASettings settings) {

        AssociationSettings as = (AssociationSettings) settings;

        FPGrowth fpGrowth = (FPGrowth) settings.getModel();

        FPGrowthModel model = fpGrowth
                .setItemsCol("text")
                .fit(dataset);

        Dataset<Row> assocRules = model.associationRules();

        this.assocRules = assocRules;
        this.fpGrowthModel = model;

        stringBuilder = stringBuilder
                .append("MinSupport: " + model.getMinSupport() + "\n")
                .append("MinConfidence: " + model.getMinConfidence() + "\n")
                .append(frequentItemsets(20))
                .append(associationRules(20));
    }

    public String frequentItemsets(int rows) {
        return fpGrowthModel.freqItemsets().showString(rows, 0, false);
    }

    public String associationRules(int rows) {
        return assocRules.showString(rows, 0, false);
    }

    public String precedentsAndConsequents(int rows, Dataset<Row> dataset) {
        // transform examines the input items against all the association rules and summarize the
        // consequents as prediction
        return fpGrowthModel.transform(dataset).showString(rows, 0, false);
    }


}
