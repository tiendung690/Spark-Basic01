package sparktemplate.dataprepare;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;


import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Klasa zawierajaca metody przygotowujace dane do wyznaczania regul asocjacyjnych.
 * <p>
 * Created by as on 13.03.2018.
 */
public class DataPrepareAssociations {

    // Remove all columns with numeric values.
    private static final boolean removeNumerics = true;
    // Remove all null columns in row.
    private static final boolean removeNull = true;
    // Logger.
    public static final String loggerName = "DataPrepareAssociations";
    private static final Logger logger = Logger.getLogger(loggerName);

    /**
     * Metoda przygotowuje dane do wyznaczania regul asocjacyjnych.
     *
     * @param data         dane
     * @param sparkSession obiekt SparkSession
     * @return przygotowane dane
     */
    public static Dataset<Row> prepareDataSet(Dataset<Row> data, SparkSession sparkSession) {

        // Find columns with StringType from dataset.
        List<String> listString = new ArrayList<>();
        for (StructField o : data.schema().fields()) {
            if (o.dataType().equals(DataTypes.StringType)) {
                listString.add(o.name());
            }
        }
        String[] stringArray = listString.toArray(new String[0]);

        // All columns are StringType.
        if (listString.size() == data.columns().length) {
            logger.info("All columns are StringType.");
            return prepareArray(data, sparkSession);
        }
        // Not all columns are StringType. Then remove them.
        else if (removeNumerics) {
            logger.info("Not all columns are StringType. Then remove them.");
            Dataset<Row> removedNumerics = data.drop(data.drop(stringArray).columns());
            return prepareArray(removedNumerics, sparkSession);
        }
        // Treat non StringType as StringType.
        else {
            logger.info("Treat non StringType as StringType.");
            // Create new StructType with only String types.
            String[] cols = data.columns();
            StructType structType = new StructType();
            for (String col : cols) {
                structType = structType.add(col, DataTypes.StringType, false);
            }
            ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);
            // Create new dataset with concatenated column names and values.
            String concatDelimiter = "-";
            Dataset<Row> columnNamesAndValuesConcatenated = data.map(value -> {
                Object[] obj = new Object[value.size()];
                for (int i = 0; i < value.size(); i++) {
                    if (value.isNullAt(i)) {
                        obj[i] = value.get(i);
                    } else {
                        obj[i] = value.get(i) + concatDelimiter + cols[i];
                    }
                }
                return RowFactory.create(obj);
            }, encoder);
            return prepareArray(columnNamesAndValuesConcatenated, sparkSession);
        }
    }

    /**
     * Metoda przygotowujaca dane w odpowiednim formacie
     *
     * @param data
     * @param sparkSession obiekt SparkSession
     * @return
     */
    private static Dataset<Row> prepareArray(Dataset<Row> data, SparkSession sparkSession) {

        logger.info("Convert dataset with string columns to one array column.");

        // Before.
        //  |-- _c0: string (nullable = true)
        //  |-- _c1: string (nullable = true)

        // After.
        //  |-- text: array (nullable = false)
        //  |    |-- element: string (containsNull = true)

        String delimiter = ",";
        Dataset<String> stringDataset;
        // Convert rows to String.
        if (removeNull) {
            stringDataset = removeNullCols(data);
        } else {
            stringDataset = data.map(row -> row.mkString(delimiter), Encoders.STRING());
        }
        // Convert String to Row with Array.
        JavaRDD<Row> rows = stringDataset.toJavaRDD().map(v1 -> RowFactory.create(new String[][]{v1.split(delimiter)}));
        // New StructType.
        StructType schema2 = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        // Create dataset.
        Dataset<Row> prepared = sparkSession.createDataFrame(rows, schema2);
        return prepared;
    }

    /**
     * Metoda usuwajaca puste kolumny w kazdym wierszu
     *
     * @param data dane
     * @return przygotowane dane
     */
    private static Dataset<String> removeNullCols(Dataset<Row> data) {

        logger.info("Remove null values.");

        // Before.
        //+-------+--------+--------+------+
        //|    _c0|     _c1|     _c2|   _c3|
        //+-------+--------+--------+------+
        //|   cola|    null|   apple|  null|
        //+-------+--------+--------+------+

        // After.
        //+--------------------+
        //|text                |
        //+--------------------+
        //|[cola, apple]       |
        //+--------------------+

        Dataset<String> removedNull = data.map(value -> {
            List list = new ArrayList<String>();
            for (int i = 0; i < value.size(); i++) {
                if (!value.isNullAt(i)) {
                    list.add(value.get(i));
                }
            }
            return list.toString()
                    .replace("[", "")
                    .replace("]", "")
                    .replaceAll(" ", "");
        }, Encoders.STRING());

        return removedNull;
    }
}
