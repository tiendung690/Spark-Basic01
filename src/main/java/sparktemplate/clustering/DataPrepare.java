package sparktemplate.clustering;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by as on 12.03.2018.
 */
public class DataPrepare {

    private static final boolean removeStrings = true; // remove all columns with String type

    static Dataset<Row> createDataSet(Row row, StructType structType, SparkSession sparkSession) {
        List<Row> rows = new ArrayList<>();
        rows.add(row);
        Dataset<Row> df2 = sparkSession.createDataFrame(rows, structType);
        //df2.printSchema();
        //df2.show();
        return df2;
    }

    public static Dataset<Row> prepareDataset(Dataset<Row> df) {

        Dataset<Row> prepared;

        // find columns with StringType from dataset
        List<String> listString = new ArrayList<>();
        List<String> listOther = new ArrayList<>();

        for (StructField o : df.schema().fields()) {
            if (o.dataType().equals(DataTypes.StringType)) {
                listString.add(o.name());
            }
            if (!o.dataType().equals(DataTypes.StringType)
                    && !o.dataType().equals(DataTypes.IntegerType)
                    && !o.dataType().equals(DataTypes.DoubleType)) {
                listOther.add(o.name());
                System.out.println("Other type: " + o.name());
            }
        }

        System.out.println("StringType in Dataset: " + listString.toString());
        System.out.println("Other DataTypes in Dataset (except int,double,string): " + listOther.toString());
        String[] stringArray = listString.toArray(new String[0]);
        String[] otherArray = listOther.toArray(new String[0]);

        if (listString.size() > 0) {
            if (removeStrings) {
                // dataset without columns with StringType
                prepared = df.drop(stringArray);
            } else {
                // dataset without columns with StringType
                Dataset<Row> df2 = df.drop(stringArray);
                //df2.printSchema();

                // dataset with StringType columns
                Dataset<Row> df3 = df.drop(df2.columns());
                //df3.printSchema();

                // create indexer for each column in dataset
                PipelineStage[] pipelineStages = new PipelineStage[df3.columns().length];

                for (int i = 0; i < pipelineStages.length; i++) {

                    // get current column name
                    String currentCol = df3.columns()[i];
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
                Dataset<Row> indexed = pipeline.fit(df).transform(df).drop(df3.columns());
                //indexed.show();

                prepared = indexed.drop(otherArray);
            }
        } else {
            System.out.println("No StringType in Dataset");
            prepared = df.drop(otherArray);
        }

        StructType schema = new StructType(new StructField[]{
                // new StructField("label", DataTypes.StringType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);


        Dataset<Row> vectorsData = prepared.map(s -> {

            double[] doubles = new double[s.size()];
            for (int i = 0; i < doubles.length; i++) {
                doubles[i] = Double.parseDouble(String.valueOf(s.get(i)).trim());
            }
            return RowFactory.create(Vectors.dense(doubles));

        }, encoder);

        // Normalize each Vector using $L^1$ norm.
        Normalizer normalizer = new Normalizer()
                .setInputCol("features")
                .setOutputCol("normFeatures")
                .setP(1.0);

        //l1NormData.show();
        return normalizer.transform(vectorsData);
    }
}
