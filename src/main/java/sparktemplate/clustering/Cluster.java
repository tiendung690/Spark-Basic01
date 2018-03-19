package sparktemplate.clustering;

import breeze.linalg.DenseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sparktemplate.DataRecord;

//Klasa pokazujaca jak implementuje sie skupienia

public class Cluster {

    //Tutaj struktury danych reprezentujace skupienie
    private Dataset<Row> ds;
    private SparkSession sparkSession;
    private DataPrepare dataPrepare;

    Cluster(SparkSession sparkSession, DataPrepare dataPrepare) {
        //Wstepna inicjacja skupienia
        this.sparkSession = sparkSession;
        this.dataPrepare = dataPrepare;
    }

    void initCluster(Dataset<Row> ds) {
        //Tutaj tworzy sie struktura skupienia
        this.ds = ds;
    }

    //  PROBLEM ?????? kazdy DataRecord jest konwertowany w przypadku petli kilka razy, lepiej raz zamienic i sprawdzac niz konwersja w tej metodzie
    public boolean checkRecord(DataRecord record) {
        //Ta metoda sprawdza, czy podany rekord znajduje się w skupieniu
        //Dataset<Row> single = DataPrepare.createDataSet(record.getRow(), record.getStructType(), sparkSession);
        Dataset<Row> single = this.dataPrepare.prepareDataset(this.dataPrepare.createDataSet(record.getRow(), record.getStructType(), sparkSession), true);
        final Object obj = single.first().get(0);
        return ds.filter(value -> value.get(0).equals(obj)).count() > 0;

//        org.apache.spark.ml.linalg.DenseVector dss = (org.apache.spark.ml.linalg.DenseVector) single.first().get(0);
//        org.apache.spark.ml.linalg.DenseVector dss2 = (org.apache.spark.ml.linalg.DenseVector) ds.first().get(0);
//        double[] dd = dss.toArray();
//        double[] dd2 = dss2.toArray();
//        for (int i = 0; i <dd.length ; i++) {
//            if(dd[i]==dd2[i]){
//                System.out.println("ok: "+dd[i]+" <-> "+dd2[i]);
//            }
//        }
    }

    public String toString() {
        return "opis";
    } //Pobranie pełnej informacji tekstowej o skupieniu w celu np. zapisu na dysk lub do bazy danych


}