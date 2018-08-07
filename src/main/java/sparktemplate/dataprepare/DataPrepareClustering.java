package sparktemplate.dataprepare;

import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

/**
 * Klasa zawierajaca metody przygotowujace dane do grupowania.
 * <p>
 * Created by as on 12.03.2018.
 */
public class DataPrepareClustering {

    private DataModelsClustering dataModelsClustering;
    // Prepare data based on existing model.
    private static final boolean isSingleDefault = true;
    // Remove symbolical columns.
    private static final boolean removeStringsDefault = true;
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
     * @param removeStrings usuwanie kolumn z wartosciami symbolicznymi
     * @return przygotowane dane
     */
    public Dataset<Row> prepareDataSet(Dataset<Row> data, boolean isSingle, boolean removeStrings){
        return prepare(data,isSingle,removeStrings);
    }

    /**
     * Metoda przygotowuje dane do grupowania.
     *
     * @param data dane
     * @return
     */
    public Dataset<Row> prepareDataSet(Dataset<Row> data){
        return prepare(data, isSingleDefault, removeStringsDefault);
    }

    private Dataset<Row> prepare(Dataset<Row> data, boolean isSingle, boolean removeStrings) {

        logger.info("isSingleDefault: " + isSingle + " removeStringsDefault: " + removeStrings);

        // Maps with symbolical and numerical values.
        // K - column, V - index
        Map<String, Integer> mapSymbolical = DataPrepare.findSymbolicalColumns(data);
        Map<String, Integer> mapNumerical = DataPrepare.findNumericalColumns(data);
        String[] symbolicalArray = mapSymbolical.keySet().toArray(new String[0]);
        String[] numericalArray = mapNumerical.keySet().toArray(new String[0]);
        // Remove unsupported types.
        data = data.drop(data.drop(symbolicalArray).drop(numericalArray).columns());
        // Prepared dataset.
        Dataset<Row> dsPrepared;
        // Symbolical values in dataset.
        if (symbolicalArray.length > 0) {
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
                // Dataset with symbolical columns.
                Dataset<Row> dsSymbolical = data.drop(numericalArray);
                // Use StringIndexer on each symbolical column with Pipeline.
                PipelineStage[] pipelineStages = new PipelineStage[dsSymbolical.columns().length];
                for (int i = 0; i < pipelineStages.length; i++) {
                    // Get current column name,
                    String currentCol = dsSymbolical.columns()[i];
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
                Dataset<Row> dsIndexed = pipelineModel.transform(data);
                dsPrepared = dsIndexed.drop(symbolicalArray);
            }
        }
        // No symbolical values in dataset.
        else {
            logger.info("No symbolical values in dataset.");
            dsPrepared = data;
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
        //Normalizer normalizer = new Normalizer()
        //        .setInputCol("features")
        //        .setOutputCol("normFeatures")
        //        .setP(1.0);
        // Transform. Dataset with features and normalized features.
        //Dataset<Row> dsNormalized = normalizer.transform(dsVectorOHE);
        //return dsNormalized;

        return dsVectorOHE;
    }
}
