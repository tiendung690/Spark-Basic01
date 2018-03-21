package sparktemplate.datasets;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import sparktemplate.DataRecord;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Klasa  <tt>MemDataSet</tt> opisuje podstawowe funkcjonalnosci zbioru danych
 * reprezentowanego w pamięci
 *
 * @author Jan G. Bazan
 * @version 1.0, luty 2018 roku
 */

public class MemDataSet{

    
    //Typy atrybutow prosze samemu ustalic, ale polecam tak jak w API WEKA
    private SparkSession sparkSession;
    private Dataset<Row> ds;

    public Dataset<Row> getDs() {
        return ds;
    }

    public MemDataSet(SparkSession sparkSession)
    {
        //Konstruktor przygotowuje struktuty do wczytania danych
        this.sparkSession=sparkSession;
    }


    public void loadDataSet(String csvFileName) //Odczytanie zbioru danych z pliku w formacie CSV (pierwszy wiersz zawiera nazwy atrybutow)
    {
        //Uwaga: Najlepiej, aby typ wartosci atrybutów był automatycznie rozpoznawany
        //Jesli bedzie to trudne dla Was, to można uzyc formatu z API WEKA (arff). Tam są zakodowane typy. 
        //Mozecie nawet uzyc struktury danych z WEKA (klasa Instances), ktora pozwala reprezentowac w pamieci dane

        this.ds = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(csvFileName);
    }
    
    //-------------
    
    public int getNoAttr() //Mozliwość sprawdzenia ile jest atrybutow (kolumn) w tablicy
    {
        return (int) ds.count();
    }
    
    public String getAttrName(int attributeIndex) //Mozliwość sprawdzenia nazwy atrybutu o podanym numerze
    {
        return ds.columns()[attributeIndex];
    }


    // indeks wiekszy od size() ???
    public DataRecord getDataRecord(int index) //Zwrocenie informacji o wierszu danych o podanym numerze
    {
        AtomicLong atIndex = new AtomicLong(index);

        JavaRDD<Row> filteredRDD = this.ds
                .toJavaRDD()
                .zipWithIndex()
                // .filter((Tuple2<Row,Long> v1) -> v1._2 >= start && v1._2 < end)
                .filter((Tuple2<Row,Long> v1) -> v1._2==atIndex.get())
                .map(r -> r._1);

        //Dataset<Row> filtered = sparkSession.createDataFrame(filteredRDD, ds.schema());
        //filteredDataFrame.show();
        System.err.println("getDataRecord at index: "+atIndex+", count:"+filteredRDD.count());
        return new DataRecord(filteredRDD.first(),ds.schema());
    }
        
    
    public int getNoRecord() //Mozliwość sprawdzenia ile jest wierszy w tablicy
    {
        return (int) ds.count();
    }
   
}

