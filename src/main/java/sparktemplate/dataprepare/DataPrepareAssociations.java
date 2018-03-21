package sparktemplate.dataprepare;

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
 * Created by as on 13.03.2018.
 */
public class DataPrepareAssociations {

    private static final boolean removeNumerics = true; // remove all columns with numeric values
    private static final boolean removeNull = true; // remove all null columns in row

    public static Dataset<Row> prepareDataSet(Dataset<Row> df, SparkSession sparkSession) {

        // find columns with StringType from dataset
        List<String> listString = new ArrayList<>();

        for (StructField o : df.schema().fields()) {
            if (o.dataType().equals(DataTypes.StringType)) {
                listString.add(o.name());
            }
        }

        System.out.println("StringType in Dataset: " + listString.toString());
        String[] stringArray = listString.toArray(new String[0]);

        // wszystkie kolumny sa string
        if (listString.size() == df.columns().length) {
            return prepareArray(df, sparkSession);
        }// nie wszystkie i chcemy usunac nie-string
        else if (removeNumerics) {
            Dataset<Row> df2 = df.drop(df.drop(stringArray).columns());
            return prepareArray(df2, sparkSession);
        } else {
            // create new structtype with only String types
            String[] cols = df.columns();
            StructType structType = new StructType();
            for (String col : cols) {
                structType = structType.add(col, DataTypes.StringType, false);
            }
            ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);
            // create new dataset with concat col names + values
            Dataset<Row> dfX = df.map(value -> {
                Object[] obj = new Object[value.size()];
                for (int i = 0; i < value.size(); i++) {
                    //System.out.println(value.get(i)+cols[i]);
                    if (value.isNullAt(i)) {
                        obj[i] = value.get(i);
                    } else {
                        obj[i] = value.get(i) + "-" + cols[i];
                    }
                }
                return RowFactory.create(obj);
            }, encoder);
            return prepareArray(dfX, sparkSession);
        }
    }

    private static Dataset<Row> prepareArray(Dataset<Row> dfX, SparkSession sparkSession) {

        Dataset<String> ds1;

        if (removeNull) {
            ds1 = removeNullCols(dfX);
        } else {
            //row -> String ","
            ds1 = dfX.map(row -> row.mkString(","), Encoders.STRING());
        }

        // row String to Array
        //JavaRDD<Row> rows = ds1.toJavaRDD().map(v1 -> RowFactory.create(new ArrayList<String>().add("elo")));
        JavaRDD<Row> rows = ds1.toJavaRDD().map(v1 -> RowFactory.create(new String[][]{v1.split(",")}));


        // new StructType
        StructType schema2 = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });

        //ExpressionEncoder<Row> encoder2 = RowEncoder.apply(schema2);
        //Dataset<Row> rows2 = ds1.map(v1 -> RowFactory.create(new String[][]{v1.split(",")}), encoder2);


        // create dataset from parts
        Dataset<Row> prepared = sparkSession.createDataFrame(rows, schema2);
        return prepared;
    }

    private static Dataset<String> removeNullCols(Dataset<Row> dfX) {

        Dataset<String> removedNull = dfX.map(value -> {
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
