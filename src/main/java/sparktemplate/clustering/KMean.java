package sparktemplate.clustering;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import sparktemplate.ASettings;
import sparktemplate.DataRecord;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by as on 12.03.2018.
 */
public class KMean implements AClustering {


    private KMeans kmeans;
    private KMeansModel model;
    private Dataset<Row> predictions;
    public SparkSession sparkSession;
    private final String prediciton = "prediction";
    public DataPrepare dataPrepare;

    public KMean(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.dataPrepare = new DataPrepare();
    }

    @Override
    public void buildClusterer(MemDataSet dataSet, ASettings settings) {
        buildCluster(dataPrepare.prepareDataset(dataSet.getDs(), false));
    }

    @Override
    public void buildClusterer(MemDataSet dataSet) {
        buildCluster(dataPrepare.prepareDataset(dataSet.getDs(), false));
    }

    @Override
    public void buildClusterer(DBDataSet dataSet, ASettings settings) {
        buildCluster(dataPrepare.prepareDataset(dataSet.getDs(), false));
    }

    @Override
    public void buildClusterer(DBDataSet dataSet) {
        buildCluster(dataPrepare.prepareDataset(dataSet.getDs(), false));
    }

    @Override
    public int clusterRecord(DataRecord dataRecord) {

        Dataset<Row> single = dataPrepare.createDataSet(dataRecord.getRow(), dataRecord.getStructType(), sparkSession);
        Dataset<Row> single_prepared = dataPrepare.prepareDataset(single,true);
        Dataset<Row> prediction = model.transform(single_prepared);
        return (int) prediction.first().get(prediction.schema().fieldIndex(prediciton));
    }

    @Override
    public Cluster getCluster(int index) {
        Cluster cluster = new Cluster(sparkSession, this.dataPrepare);
        cluster.initCluster(predictions.filter(predictions.col(prediciton).equalTo(index)));
        return cluster;
    }

    @Override
    public int getNoCluster() {
        return model.clusterCenters().length;
    }

    @Override
    public void saveClusterer(String fileName) throws IOException {
        this.predictions.write().mode(SaveMode.Overwrite).json(fileName);
        System.out.println("saveClusterer: "+fileName);
    }

    @Override
    public void loadClusterer(String fileName) throws IOException {
        this.predictions = sparkSession.read().json(fileName);
        System.out.println("loadClusterer: "+fileName);
    }

    private void buildCluster(Dataset<Row> ds) {

        // Trains a k-means model.
        KMeans kmeans = new KMeans().setK(4).setSeed(10L).setFeaturesCol("normFeatures");
        KMeansModel model = kmeans.fit(ds);

        // Make predictions
        Dataset<Row> predictions = model.transform(ds);

        predictions.show(false);
        predictions.printSchema();

        this.kmeans = kmeans;
        this.model = model;
        this.predictions = predictions;

    }
}
