package sparktemplate.dataprepare;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * Klasa zawierajaca metody przygotowujace dane do klasyfikacji.
 *
 * Created by as on 19.03.2018.
 */
public class DataPrepareClassification {

    private static final boolean removeStrings = false;

    public static Dataset<Row> prepareLabeledPoint(Dataset<Row> ds) {


        // without label
        Dataset<Row> noLabel = ds.drop(ds.columns()[ds.columns().length - 1]);

        // only label
        Dataset<Row> dsLabel = ds.drop(noLabel.columns());
        //dsLabel.show();

        // find symbolical and numerical
        List<String> listStr = new ArrayList();
        List<String> listNum = new ArrayList();

        for (StructField o : noLabel.schema().fields()) {
            if (o.dataType().equals(DataTypes.StringType)) {
                listStr.add(o.name());
            } else {
                listNum.add(o.name());
            }
        }

        String[] num = listNum.toArray(new String[0]);
        String[] str = listStr.toArray(new String[0]);

        // only numerical without label
        Dataset<Row> dsNum = noLabel.drop(str);

        // only symbolical without label
        Dataset<Row> dsStr = noLabel.drop(num);


        String[] strNoLabel = dsStr.columns();
        String[] numNoLabel = dsNum.columns();


        Dataset<Row> data;


        if (numNoLabel.length + 1 == ds.columns().length || removeStrings) { // ONLY NUMERICAL

            VectorAssembler assembler2 = new VectorAssembler()
                    .setInputCols(numNoLabel)
                    .setOutputCol("features");

            Dataset<Row> vectorNum = assembler2.transform(ds).drop(numNoLabel);//.drop(dsNum.columns());
            //vectorNum.show();
            data = vectorNum.withColumnRenamed("class", "label");

        } else {

            PipelineStage[] pipelineStages = new PipelineStage[strNoLabel.length];

            for (int i = 0; i < pipelineStages.length; i++) {
                // get current column name
                String currentCol = strNoLabel[i];
                // create indexer on column
                StringIndexer indexer = new StringIndexer()
                        .setInputCol(currentCol)
                        .setOutputCol(currentCol + "*");
                // add indexer to pipeline
                pipelineStages[i] = indexer;
            }

            // set stages to pipeline
            Pipeline pipeline = new Pipeline().setStages(pipelineStages);
            // fit and transform, drop old columns
            PipelineModel pipelineModel = pipeline.fit(ds);

            // wszystko
            Dataset<Row> dsPip = pipelineModel.transform(ds);
            //dsPip.show();

            // a1*, a2*, a3* ....
            Dataset<Row> afterIndexer = dsPip.drop(ds.columns());
            //afterIndexer.show();


            // ONE-HOT-ENCODER *********************************************
            String[] afterStringIndexer = afterIndexer.columns();
            String[] afterOneHot = new String[afterStringIndexer.length];

            for (int i = 0; i < afterOneHot.length; i++) {
                afterOneHot[i] = new StringBuffer().append(afterStringIndexer[i]).append("*").toString();
            }


            OneHotEncoderEstimator encoderHot = new OneHotEncoderEstimator()
                    .setInputCols(afterStringIndexer)
                    .setOutputCols(afterOneHot)
                    //.setHandleInvalid("keep") // keep invalid and assign extra value
                    .setDropLast(false);  // avoid removing last val

            /// MODEL
            OneHotEncoderModel oneHotEncoderModel = encoderHot.fit(dsPip);

            Dataset<Row> afterOneHotEncoder = oneHotEncoderModel.transform(dsPip).drop(afterIndexer.columns());
            //afterOneHotEncoder.show();

            // VECTOR FORM ONE-HOT

            VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(afterOneHot)
                    .setOutputCol("featuresOHE");


            Dataset<Row> vectorOHE = assembler.transform(afterOneHotEncoder);//.drop(afterOneHotEncoder.columns());
            //vectorOHE.show();
            //vectorOHE.printSchema();


            if (strNoLabel.length + 1 == ds.columns().length) { // ONLY SYMBOLICAL
                // DROP
                String[] colsForDelete = afterOneHotEncoder.drop(dsLabel.columns()).columns();
                Dataset<Row> dsFeaturesLabel = vectorOHE.drop(colsForDelete);
                //dsFeaturesLabel.show();
                data = dsFeaturesLabel
                        .withColumnRenamed("class", "label")
                        .withColumnRenamed("featuresOHE", "features");

            } else { // MIXED SYMBOLICAL AND NUMERICAL

                VectorAssembler assembler2 = new VectorAssembler()
                        .setInputCols(numNoLabel)
                        .setOutputCol("featuresNUM");

                Dataset<Row> vectorNum = assembler2.transform(vectorOHE);//.drop(dsNum.columns());
                //vectorNum.show();

                // Connect vectorOHE and vectorNum

                VectorAssembler assembler3 = new VectorAssembler()
                        .setInputCols(new String[]{"featuresOHE", "featuresNUM"})
                        .setOutputCol("features");

                Dataset<Row> vectorAll = assembler3.transform(vectorNum);//.drop(dsNum.columns());

                // DROP
                String[] colsForDelete = vectorNum.drop(dsLabel.columns()).columns();

                Dataset<Row> dsFeaturesLabel = vectorAll.drop(colsForDelete);
                //dsFeaturesLabel.show();

                data = dsFeaturesLabel.withColumnRenamed("class", "label");
            }
        }
        return data;
    }
}
