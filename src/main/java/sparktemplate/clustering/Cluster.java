package sparktemplate.clustering;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.datarecord.DataRecord;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClustering;


/**
 * Klasa pokazujaca jak implementuje sie skupienia
 */
public class Cluster {

    private Dataset<Row> ds;
    private SparkSession sparkSession;
    private DataPrepareClustering dataPrepareClustering;
    private boolean removeStrings;

    Cluster(SparkSession sparkSession, DataPrepareClustering dataPrepareClustering, boolean removeStrings) {
        this.sparkSession = sparkSession;
        this.dataPrepareClustering = dataPrepareClustering;
        this.removeStrings = removeStrings;
    }

    /**
     * Metoda wypelniajaca skupienia do prywantego pola.
     *
     * @param ds skupienia
     */
    void initCluster(Dataset<Row> ds) {
        this.ds = ds;
    }

    /**
     * * Metoda sprawdza obecnosc rekordu w skupieniu
     *
     * @param record     rekord danych
     * @param isPrepared - dane przygotowane
     * @return obecnosc
     */
    public boolean checkRecord(DataRecord record, boolean isPrepared) {
        Dataset<Row> single;
        if (isPrepared) {
            single = DataPrepare.createDataSet(record.getRow(), record.getStructType(), sparkSession);
        } else {
            single = dataPrepareClustering.prepareDataSet(DataPrepare.createDataSet(record.getRow(), record.getStructType(), sparkSession), true, removeStrings);
        }
        final Object obj = single.first().get(0);
        return ds.filter(value -> value.get(0).equals(obj)).count() > 0;
    }

    /**
     * Metoda zwracajaca informacje tekstowe o skupieniu w celu np. zapisu na dysk lub do bazy danych
     *
     * @return informacje
     */
    public String toString() {
        return "testcluster size: " + ds.count();
    }


}