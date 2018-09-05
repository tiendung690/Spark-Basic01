package sparktemplate.dataprepare;

import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.linalg.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

/**
 * Klasa zawierajaca metody przygotowujace wstepnie dane.
 *
 * Created by as on 21.03.2018.
 */
public class DataPrepare {

    // Logger.
    public static final String loggerName = "DataPrepare";
    private static final Logger logger = Logger.getLogger(loggerName);


    /**
     * Metoda zamieniajaca wektory na geste(dense). Uzyto tutaj UserDefinedFunction.
     *
     * @param data
     * @param featuresCol - Kolumna zawierajaca wektory.
     * @return
     */
    public static Dataset<Row> convertVectorColToDense(Dataset<Row> data, String featuresCol) {
        // Prepare udf.
        UserDefinedFunction mode = udf(
                (org.apache.spark.ml.linalg.Vector v) -> v.toDense(), SQLDataTypes.VectorType()
        );
        // Convert.
        Dataset<Row> converted = data
                .withColumn("udf", mode.apply(col(featuresCol)))
                .drop(featuresCol)
                .withColumnRenamed("udf", featuresCol);
        return converted;
    }

    /**
     * Metoda redukujaca wymiary danych.
     *
     * @param data
     * @param featuresCol - Nazwa kolumny z danymi.
     * @param reducedDimensionsCol - Nowa kolumna ze zredukowanymi danymi.
     * @param dimensions - Liczba oczekiwanych wymiarow.
     * @return
     */
    public static Dataset<Row> reduceDimensions(Dataset<Row> data, String featuresCol, String reducedDimensionsCol, int dimensions){
        PCA pca = new PCA()
                .setInputCol(featuresCol)
                .setOutputCol(reducedDimensionsCol)
                .setK(dimensions);
        PCAModel pcaModel = pca.fit(data);
        Dataset<Row> result = pcaModel.transform(data);
        return result;
    }

    /**
     * Metoda zwracajaca probke danych.
     *
     * @param data
     * @param fraction - czesc danych (0-1)
     * @return
     */
    public static Dataset<Row> sampleData(Dataset<Row> data, double fraction){
        return data.sample(fraction, 10L);
    }

    /**
     * Metoda zwielokrotniajaca dane.
     *
     * @param data
     * @param multiplier - mnoznik
     * @return
     */
    public static Dataset<Row> multiplyData(Dataset<Row> data, int multiplier){
        Dataset<Row> ds = data;
        for (int i = 1; i < multiplier; i++) {
            ds=ds.union(data);
        }
        return ds;
    }

    /**
     * Metoda tworzaca Dataset w oparciu o czesci skladowe innego Dataseta.
     *
     * @param row          dane
     * @param structType   struktura
     * @param sparkSession obiekt SparkSession
     * @return zbior danych
     */
    public static Dataset<Row> createDataSet(Row row, StructType structType, SparkSession sparkSession) {
        logger.info("Create dataset.");
        List<Row> rows = new ArrayList<>();
        rows.add(row);
        return sparkSession.createDataFrame(rows, structType);
    }

    /**
     * Metoda zwracajaca mape nazw koolumn wartosci i ich indeksy.
     *
     * @param data - dane
     * @return zmapowane wartosci
     */
    public static Map<String, Integer> findSymbolicalColumns(Dataset<Row> data) {
        // Maps with symbolical values.
        // K - column, V - index
        Map<String, Integer> mapSymbolical = new HashMap<>();
        // Find symbolical values and their indexes in dataset.
        int j = 0;
        for (StructField o : data.schema().fields()) {
            if (o.dataType().equals(DataTypes.StringType)) {
                mapSymbolical.put(o.name(), j);
            }
            j++;
        }
        logger.info("Symbolical values:" + mapSymbolical.keySet().toString());
        return mapSymbolical;
    }

    /**
     * Metoda usuwajaca kolumny z wartosciami symbolicznymi.
     *
     * @param data
     * @return
     */
    public static Dataset<Row> removeSymbolicalCols(Dataset<Row> data){
        return data.drop(findSymbolicalColumns(data).keySet().toArray(new String[0]));
    }

    /**
     * Metoda zwracajaca mape nazw koolumn wartosci i ich indeksy.
     *
     * @param data - dane
     * @return zmapowane wartosci
     */
    public static Map<String, Integer> findNumericalColumns(Dataset<Row> data) {
        // Maps with numerical values.
        // K - column, V - index
        Map<String, Integer> mapNumerical = new HashMap<>();
        // Find numerical values and their indexes in dataset.
        int j = 0;
        for (StructField o : data.schema().fields()) {
            if (o.dataType().equals(DataTypes.IntegerType)
                    || o.dataType().equals(DataTypes.DoubleType)
                    || o.dataType().equals(DataTypes.FloatType)
                    || o.dataType().equals(DataTypes.LongType)
                    || o.dataType().equals(DataTypes.ShortType)) {
                mapNumerical.put(o.name(), j);
            }
            j++;
        }
        logger.info("Numerical values:" + mapNumerical.keySet().toString());
        return mapNumerical;
    }

    /**
     * Metoda usuwajaca kolumny z wartosciami numerycznymi.
     *
     * @param data
     * @return
     */
    public static Dataset<Row> removeNumericalCols(Dataset<Row> data){
        return data.drop(findNumericalColumns(data).keySet().toArray(new String[0]));
    }


    /**
     * Metoda wypelniajaca brakujace dane.
     *
     * @param ds zbior danych z brakujacymi danymi
     * @return wypelniony zbior danych
     */
    public static Dataset<Row> fillMissingValues(Dataset<Row> ds) {

        logger.info("Fill missing values.");

        // Maps with symbolical and numerical values.
        // K - column, V - index
        Map<String, Integer> mapSymbolical = findSymbolicalColumns(ds);
        Map<String, Integer> mapNumerical = findNumericalColumns(ds);
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
            Double avg = sum / ss;
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
        return dsWithoutNulls;
    }
}
