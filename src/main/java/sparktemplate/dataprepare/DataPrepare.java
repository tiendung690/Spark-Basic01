package sparktemplate.dataprepare;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * Created by as on 21.03.2018.
 */
public class DataPrepare {

    public static Dataset<Row> createDataSet(Row row, StructType structType, SparkSession sparkSession) {
        List<Row> rows = new ArrayList<>();
        rows.add(row);
        //df2.printSchema();
        //df2.show();
        return sparkSession.createDataFrame(rows, structType);
    }


    public static Dataset<Row> fillMissingValues(Dataset<Row> ds) {

        // find symbolical and numerical values in dataset
        List<String> listStr = new ArrayList(); // column name
        List<Integer> listStrIndex = new ArrayList(); // column index
        List<String> listNum = new ArrayList(); // column name
        List<Integer> listNumIndex = new ArrayList(); // column index

        int ii = 0;
        for (StructField o : ds.schema().fields()) {
            if (o.dataType().equals(DataTypes.StringType) || o.dataType().equals(DataTypes.DateType)) {
                listStr.add(o.name());
                listStrIndex.add(ii);
                ii++;
            } else if (o.dataType().equals(DataTypes.IntegerType)
                    || o.dataType().equals(DataTypes.DoubleType)
                    || o.dataType().equals(DataTypes.StringType)
                    || o.dataType().equals(DataTypes.FloatType)
                    || o.dataType().equals(DataTypes.LongType)
                    || o.dataType().equals(DataTypes.ShortType)){
                listNum.add(o.name());
                listNumIndex.add(ii);
                ii++;
            } else {
                ii++;
            }
        }

        // K - column, V - replacement value
        Map<String, Object> mapReplacementValues = new HashMap<>();

        ///////////////////////  NUMERICAL VALUES

        // count col values (accept nulls)
        //long ss = ds.map(value -> 1, Encoders.INT()).reduce((v1, v2) -> v1+v2);

        for (int i = 0; i < listNumIndex.size(); i++) {

            int colId = listNumIndex.get(i);

            // count values (without nulls)
            long ss = ds.filter(value -> !value.isNullAt(colId)).count();
                   // .map(value -> 1, Encoders.INT()).reduce((v1, v2) -> v1+v2);

            // sum values
            Double sum = ds.filter(value -> !value.isNullAt(colId))
                    .map(value -> Double.parseDouble(value.get(colId).toString()), Encoders.DOUBLE())
                    .reduce((v1, v2) -> v1 + v2);

            //long ss = ds.filter(value -> !value.isNullAt(colId)).map(value -> 1, Encoders.INT()).reduce((v1, v2) -> v1+v2);

            Double avg = sum/ss;
            mapReplacementValues.put(listNum.get(i), avg);
        }

        /////////////////////////////  SYMBOLICAL VALUES

        for (int i = 0; i < listStrIndex.size(); i++) {

            int colId = listStrIndex.get(i);

            Dataset<String> words = ds
                    .filter(value -> !value.isNullAt(colId))
                    .flatMap(s -> Arrays.asList(s.get(colId).toString().split(" ")).iterator(), Encoders.STRING())
                    //.filter(s -> !s.isEmpty())
                    .coalesce(1); //one partition (parallelism level)

            // words.show();

            Dataset<Row> t2 = words.groupBy("value")
                    .count()
                    .toDF("word", "count");

            t2 = t2.sort(functions.desc("count"));

            String commonValue = (String) t2.first().get(0);
            mapReplacementValues.put(listStr.get(i), commonValue);
        }

        //System.out.println("Replacement values (column, value) :"+Arrays.asList(mapReplacementValues));

        // Fill missing values
        Dataset<Row> dsWithoutNulls = ds.na().fill(mapReplacementValues);
        //dsWithoutNulls.show();
        return  dsWithoutNulls;

    }
}
