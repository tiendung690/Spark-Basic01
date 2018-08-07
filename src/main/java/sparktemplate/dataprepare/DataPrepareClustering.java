package sparktemplate.dataprepare;

import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Klasa zawierajaca metody przygotowujace dane do grupowania.
 * <p>
 * Created by as on 12.03.2018.
 */
public class DataPrepareClustering {

    private DataModelsClustering dataModelsClustering;
    // Logger.
    public static final String loggerName = "DataPrepareClustering";
    private static final Logger logger = Logger.getLogger(loggerName);

    public DataPrepareClustering() {
        this.dataModelsClustering = new DataModelsClustering();
    }

    /**
     * Metoda przygotowuje dane do grupowania.
     *
     * @param data          dane
     * @param isSingle      dane zawieraja tylko jeden wiersz (wyznaczenie skupiska dla pojedynczego obiektu)
     * @param removeStrings usuwanie kolumn z typem String
     * @return przygotowane dane
     */
    public Dataset<Row> prepareDataSet(Dataset<Row> data, boolean isSingle, boolean removeStrings) {


        logger.info("isSingle: " + isSingle + " removeStrings: " + removeStrings);

        // Find columns with StringType from dataset.
        List<String> listSymbolical = new ArrayList<>();
        List<String> listOtherTypes = new ArrayList<>();

        for (StructField o : data.schema().fields()) {
            if (o.dataType().equals(DataTypes.StringType)) {
                listSymbolical.add(o.name());
            }
            if (!o.dataType().equals(DataTypes.StringType)
                    && !o.dataType().equals(DataTypes.IntegerType)
                    && !o.dataType().equals(DataTypes.DoubleType)) {
                listOtherTypes.add(o.name());
            }
        }

        //System.out.println("StringType in Dataset: " + listString.toString());
        //System.out.println("Other DataTypes in Dataset (except int,double,string): " + listOther.toString());
        String[] symbolicalArray = listSymbolical.toArray(new String[0]);
        String[] otherTypesArray = listOtherTypes.toArray(new String[0]);

        Dataset<Row> dsPrepared;

        // Symbolical values in dataset.
        if (listSymbolical.size() > 0) {
            logger.info("Symbolical values in dataset.");
            // Remove symbolical values.
            if (removeStrings) {
                logger.info("Remove symbolical values.");
                // Delete columns with StringType.
                dsPrepared = data.drop(symbolicalArray);
            }
            // Keep symbolical values.
            else {
                logger.info("Keep symbolical values.");
                // Dataset without symbolical columns.
                Dataset<Row> dsSymbolical = data.drop(symbolicalArray);
                // Dataset with numerical columns.
                Dataset<Row> dsNumerical = data.drop(dsSymbolical.columns());
                // Use StringIndexer on each symbolical column with Pipeline.
                PipelineStage[] pipelineStages = new PipelineStage[dsNumerical.columns().length];
                for (int i = 0; i < pipelineStages.length; i++) {
                    // Get current column name,
                    String currentCol = dsNumerical.columns()[i];
                    // Create indexer on column.
                    StringIndexer indexer = new StringIndexer()
                            .setInputCol(currentCol)
                            .setOutputCol(currentCol + "*");
                    // Add indexer to pipeline.
                    pipelineStages[i] = indexer;
                }
                // Set stages to pipeline.
                Pipeline pipeline = new Pipeline().setStages(pipelineStages);
                // Fit and transform, drop old columns.
                PipelineModel pipelineModel;
                // Prepare data next time with existing models.
                if (isSingle) {
                    pipelineModel = this.dataModelsClustering.getPipelineModel();
                    if (pipelineModel==null){
                        throw new RuntimeException("The model does not exist.");
                    }
                }
                // Prepare data first time.
                else {
                    pipelineModel = pipeline.fit(data);
                    this.dataModelsClustering.setPipelineModel(pipelineModel);
                }
                // Transform and drop unnecessary columns.
                Dataset<Row> dsIndexed = pipelineModel.transform(data).drop(dsNumerical.columns());
                dsPrepared = dsIndexed.drop(otherTypesArray);
            }
        }
        // No symbolical values in dataset.
        else {
            logger.info("No symbolical values in dataset.");
            dsPrepared = data.drop(otherTypesArray);
        }


        // Column names created by StringIndexer.
        String[] afterStringIndexer = dsPrepared.drop(data.columns()).columns();
        // Future column names created by OneHotEncoder.
        String[] afterOneHot = new String[afterStringIndexer.length];

        for (int i = 0; i < afterOneHot.length; i++) {
            afterOneHot[i] = new StringBuffer().append(afterStringIndexer[i]).append("*").toString();
        }

        // OneHotEncoder Maps a column of category indices to a column of binary vectors.
        OneHotEncoderEstimator encoderHot = new OneHotEncoderEstimator()
                .setInputCols(afterStringIndexer)
                .setOutputCols(afterOneHot)
                //.setHandleInvalid("keep") // keep invalid and assign extra value
                .setDropLast(false);  // avoid removing last val

        OneHotEncoderModel oneHotEncoderModel;
        // Prepare data next time with existing models.
        if (isSingle) {
            oneHotEncoderModel = this.dataModelsClustering.getOneHotEncoderModel();
            if (oneHotEncoderModel==null){
                throw new RuntimeException("The model does not exist.");
            }
        }
        // Prepare data first time.
        else {
            oneHotEncoderModel = encoderHot.fit(dsPrepared);
            this.dataModelsClustering.setOneHotEncoderModel(oneHotEncoderModel);
        }

        // Transform and drop remained StringIndexer columns.
        Dataset<Row> dsAfterOneHotEncoder = oneHotEncoderModel.transform(dsPrepared).drop(afterStringIndexer);

        // Convert OneHotEncoder columns to Vector.
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(dsAfterOneHotEncoder.columns())
                .setOutputCol("features");
        // Transform.
        Dataset<Row> dsVectorOHE = assembler.transform(dsAfterOneHotEncoder).drop(dsAfterOneHotEncoder.columns());
        // Normalize each Vector.
        Normalizer normalizer = new Normalizer()
                .setInputCol("features")
                .setOutputCol("normFeatures")
                .setP(1.0);
        // Transform. Dataset with features and normalized features.
        Dataset<Row> dsNormalized = normalizer.transform(dsVectorOHE);
        return dsNormalized;
    }
}
