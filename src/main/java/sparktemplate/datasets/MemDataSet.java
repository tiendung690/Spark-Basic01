package sparktemplate.datasets;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import sparktemplate.DataRecord;

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

    /**
     * Konstruktor inicjalizujacy obiekt MemDataSet.
     *
     * @param sparkSession obiekt sparkSession
     */
    public MemDataSet(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public Dataset<Row> getDs() {
        return ds;
    }

    /**
     * Odczytanie zbioru danych z pliku w formacie CSV (pierwszy wiersz zawiera nazwy atrybutow)
     *
     * @param csvFileName sciezka do pliku
     */
    public void loadDataSet(String csvFileName) {
        this.ds = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(csvFileName);
    }

    /**
     * Odczytanie zbioru danych z pliku w formacie CSV (pierwszy wiersz zawiera nazwy atrybutow)
     *
     * @param csvFileName sciezka do pliku
     * @param header zawiera nazwy kolumn
     * @param inferSchema
     */
    public void loadDataSet(String csvFileName, boolean header, boolean inferSchema) {
        this.ds = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", header)
                .option("inferSchema", inferSchema)
                .load(csvFileName);
    }

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

