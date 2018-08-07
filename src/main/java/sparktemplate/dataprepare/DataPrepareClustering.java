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
 *
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
     * @param data dane
     * @param isSingle dane zawieraja tylko jeden wiersz (wyznaczenie skupiska dla pojedynczego obiektu)
     * @param removeStrings usuwanie kolumn z typem String
     * @return przygotowane dane
     */
    public Dataset<Row> prepareDataSet(Dataset<Row> data, boolean isSingle, boolean removeStrings) {


        logger.info("isSingle: "+isSingle+" removeStrings: "+removeStrings);

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


        ///  ONE-HOT-ENCODER
        String[] afterStringIndexer = dsPrepared.drop(data.columns()).columns();
        String[] afterStringIndexer2 = new String[afterStringIndexer.length];

        for (int i = 0; i < afterStringIndexer2.length; i++) {
            afterStringIndexer2[i] = new StringBuffer().append(afterStringIndexer[i]).append("*").toString();
        }

        OneHotEncoderEstimator encoderHot = new OneHotEncoderEstimator()
                .setInputCols(afterStringIndexer)
                .setOutputCols(afterStringIndexer2)
                //.setHandleInvalid("keep") // keep invalid and assign extra value
                .setDropLast(false);  // avoid removing last val

        /// MODEL
        OneHotEncoderModel oneHotEncoderModel;

        if (isSingle) {
            oneHotEncoderModel = this.dataModelsClustering.getOneHotEncoderModel();
        } else {
            oneHotEncoderModel = encoderHot.fit(dsPrepared);
            this.dataModelsClustering.setOneHotEncoderModel(oneHotEncoderModel);
        }

        Dataset<Row> encoded = oneHotEncoderModel.transform(dsPrepared).drop(afterStringIndexer);
        //////////////////////////////////////////////////////////////////////////////////////


        // EXAMPLE 2 BETTER/////////////////////////////////////////////////////////////////////////////
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(encoded.columns())
                .setOutputCol("features");
        Dataset<Row> output = assembler.transform(encoded).drop(encoded.columns());
        output.show(false);
        //////////////////////////////////////////////////////////////////////////////////////////


        // Normalize each Vector.
        Normalizer normalizer = new Normalizer()
                .setInputCol("features")
                .setOutputCol("normFeatures")
                .setP(1.0);
        // Transform. Dataset with features and normalized features.
        Dataset<Row> dsNormalized = normalizer.transform(output);
        dsNormalized.show(false);
        return dsNormalized;
    }
}
