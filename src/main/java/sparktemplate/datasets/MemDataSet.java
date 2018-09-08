package sparktemplate.datasets;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import sparktemplate.datarecord.DataRecord;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Klasa  <tt>MemDataSet</tt> opisuje podstawowe funkcjonalnosci zbioru danych
 * reprezentowanego w pamieci
 *
 * @author Jan G. Bazan
 * @version 1.0, luty 2018 roku
 */

public class MemDataSet implements ADataSet {

    private SparkSession sparkSession;
    private Dataset<Row> ds;

    public MemDataSet() {
    }

    public MemDataSet(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public Dataset<Row> getDs() {
        return ds;
    }

    public MemDataSet setDs(Dataset<Row> ds) {
        this.ds = ds;
        return this;
    }

    /**
     * Odczytanie zbioru danych z pliku w formacie CSV (pierwszy wiersz zawiera nazwy atrybutow)
     * Domyslny delimiter to ",".
     *
     * @param path sciezka do pliku
     */
    public void loadDataSetCSV(String path) {
        this.ds = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(path);
    }

    /**
     *
     * @param path
     * @param delimiter CSV delimiter.
     */
    public void loadDataSetCSV(String path, String delimiter) {
        this.ds = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .option("inferSchema", true)
                .option("delimiter", delimiter)
                .load(path);
    }

    /**
     * Odczytanie zbioru danych z pliku w formacie CSV (pierwszy wiersz zawiera nazwy atrybutow)
     *
     * @param path        sciezka do pliku
     * @param header      zawiera nazwy kolumn
     * @param inferSchema
     */
    public void loadDataSetCSV(String path, boolean header, boolean inferSchema) {
        this.ds = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", header)
                .option("inferSchema", inferSchema)
                .load(path);
    }

    public static void saveDataSetCSV(String path, Dataset<Row> dataset) {dataset.write().csv(path);}

    public void loadDataSetPARQUET(String path) {this.ds = sparkSession.read().parquet(path);}

    // -----------------------------------IMPORTANT--------------------------------------------------
    // Use parquet format when features are SparseVector or save as JSON with provided schema.
    public static void saveDataSetPARQUET(String path, Dataset<Row> dataset) {dataset.write().parquet(path);}

    public void loadDataSetOrc(String path) {this.ds = sparkSession.read().orc(path);}

    public static void saveDataSetOrc(String path, Dataset<Row> dataset) {dataset.write().orc(path);}

    public void loadDataSetJSON(String path) {this.ds = sparkSession.read().json(path);}

    public void loadDataSetJSON(String path, StructType schema) {this.ds = sparkSession.read().schema(schema).json(path);}

    public static void saveDataSetJSON(String path, Dataset<Row> dataset) {dataset.write().json(path);}

    /**
     * Metoda zwracajaca ilosc atrybutow (kolumn) w tablicy
     *
     * @return liczba atrybutow
     */
    public int getNoAttr() {
        return (int) ds.count();
    }

    /**
     * Metoda sprawdzajaca nazwe atrybutu o podanym numerze
     *
     * @param attributeIndex
     * @return
     */
    public String getAttrName(int attributeIndex) {
        return ds.columns()[attributeIndex];
    }

    /**
     * Metoda zwracajaca wiersz o podanym numerze.
     *
     * @param index numer wiersza
     * @return pojedynczy wiersz jako obiekt DataRecord
     */
    public DataRecord getDataRecord(int index) {
        AtomicLong atIndex = new AtomicLong(index);

        JavaRDD<Row> filteredRDD = this.ds
                .toJavaRDD()
                .zipWithIndex()
                // .filter((Tuple2<Row,Long> v1) -> v1._2 >= start && v1._2 < end)
                .filter((Tuple2<Row, Long> v1) -> v1._2 == atIndex.get())
                .map(r -> r._1);

        System.err.println("getDataRecord at index: " + atIndex + ", count:" + filteredRDD.count());
        return new DataRecord(filteredRDD.first(), ds.schema());
    }

    /**
     * Metoda sprawdzajaca ile jest wierszy w tablicy
     *
     * @return liczba wierszy w tablicy (Za pomoca metody sparka .count())
     */
    public int getNoRecord() //Mozliwość sprawdzenia ile jest wierszy w tablicy
    {
        return (int) ds.count();
    }

}

