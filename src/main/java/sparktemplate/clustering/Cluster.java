package sparktemplate.clustering;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.DataRecord;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClustering;

//Klasa pokazujaca jak implementuje sie skupienia

public class Cluster {

    //Tutaj struktury danych reprezentujace skupienie
    private Dataset<Row> ds;
    private SparkSession sparkSession;
    private DataPrepareClustering dataPrepareClustering;

    Cluster(SparkSession sparkSession, DataPrepareClustering dataPrepareClustering) {
        //Wstepna inicjacja skupienia
        this.sparkSession = sparkSession;
        this.dataPrepareClustering = dataPrepareClustering;
    }

    void initCluster(Dataset<Row> ds) {
        //Tutaj tworzy sie struktura skupienia
        this.ds = ds;
    }

    public boolean checkRecord(DataRecord record) {
        //Ta metoda sprawdza, czy podany rekord znajduje się w skupieniu
        Dataset<Row> single = this.dataPrepareClustering.prepareDataset(DataPrepare.createDataSet(record.getRow(), record.getStructType(), sparkSession), true, false);
        final Object obj = single.first().get(0);
        return ds.filter(value -> value.get(0).equals(obj)).count() > 0;
    }

    public String toString() {
        return "opis";
    } //Pobranie pełnej informacji tekstowej o skupieniu w celu np. zapisu na dysk lub do bazy danych


}