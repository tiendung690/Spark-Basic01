package sparktemplate.dataprepare;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * Klasa zawierajaca metody przygotowujace wstepnie dane.
 *
 * Created by as on 21.03.2018.
 */
public class DataPrepare {

    /**
     * Metoda tworzaca Dataset w oparciu o czesci skladowe innego Dataseta.
     *
     * @param row dane
     * @param structType struktura
     * @param sparkSession obiekt SparkSession
     * @return zbior danych
     */
    public static Dataset<Row> createDataSet(Row row, StructType structType, SparkSession sparkSession) {
        List<Row> rows = new ArrayList<>();
        rows.add(row);
        return sparkSession.createDataFrame(rows, structType);
    }


    /**
     * Metoda wypelniajaca brakujace dane.
     *
     * @param ds zbior danych z brakujacymi danymi
     * @return wypelniony zbior danych
     */
    public static Dataset<Row> fillMissingValues(Dataset<Row> ds) {

        // Maps with symbolical and numerical values.
        // K - column, V - index
        Map<String, Integer> mapSymbolical = new HashMap<>();
        Map<String, Integer> mapNumerical = new HashMap<>();

        // Find symbolical and numerical values in dataset.
        int j = 0;
        for (StructField o : ds.schema().fields()) {
            if (o.dataType().equals(DataTypes.StringType) || o.dataType().equals(DataTypes.DateType)) {
                mapSymbolical.put(o.name(), j);
            } else if (o.dataType().equals(DataTypes.IntegerType)
                    || o.dataType().equals(DataTypes.DoubleType)
                    || o.dataType().equals(DataTypes.StringType)
                    || o.dataType().equals(DataTypes.FloatType)
                    || o.dataType().equals(DataTypes.LongType)
                    || o.dataType().equals(DataTypes.ShortType)){
                mapNumerical.put(o.name(),j);
            }
            j++;
        }

        // Map with replacement values.
        // K - column, V - replacement value
        Map<String, Object> mapReplacementValues = new HashMap<>();

        // Find missing numerical values replacement.
        mapNumerical.entrySet().forEach(s -> {
            //Column index.
            int colId = s.getValue();
            // Count values (without nulls).
            long ss = ds.filter(value -> !value.isNullAt(colId)).count();
            // Sum values.
            Double sum = ds.filter(value -> !value.isNullAt(colId))
                    .map(value -> Double.parseDouble(value.get(colId).toString()), Encoders.DOUBLE())
                    .reduce((v1, v2) -> v1 + v2);
            // Compute and round avg.
            Double avg = sum/ss;
            BigDecimal avgRounded = new BigDecimal(avg);
            avgRounded = avgRounded.setScale(2, RoundingMode.HALF_UP);
            mapReplacementValues.put(s.getKey(), avgRounded.doubleValue());
        });


        // Find missing symbolical values replacement.
        mapSymbolical.entrySet().forEach(s -> {
            // Column index.
            int colId = s.getValue();
            // Filter symbolical values.
            Dataset<String> words = ds
                    .filter(value -> !value.isNullAt(colId))
                    .flatMap(value -> Arrays.asList(value.get(colId).toString().split(" ")).iterator(), Encoders.STRING())
                    .coalesce(1);
            // Count values frequency.
            Dataset<Row> t2 = words.groupBy("value")
                    .count()
                    .toDF("word", "count");
            // Sort values.
            t2 = t2.sort(functions.desc("count"));
            // Get most frequent value.
            String commonValue = (String) t2.first().get(0);
            mapReplacementValues.put(s.getKey(), commonValue);
        });

        // Print replacement values.
        //System.out.println("Replacement values (column, value) :"+Arrays.asList(mapReplacementValues));

        // Fill missing values
        Dataset<Row> dsWithoutNulls = ds.na().fill(mapReplacementValues);
        return  dsWithoutNulls;
    }
}
