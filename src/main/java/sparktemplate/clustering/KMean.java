package sparktemplate.clustering;

import org.apache.spark.ml.clustering.ClusteringSummary;
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
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.ADataSet;
import sparktemplate.strings.ClusteringStrings;

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
    private DataPrepareClustering dataPrepareClustering;
    private final boolean removeStrings = true;
    private StringBuilder stringBuilder;

    public KMean(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.dataPrepareClustering = new DataPrepareClustering();
        this.stringBuilder = new StringBuilder();
    }

    public StringBuilder getStringBuilder() {
        return stringBuilder;
    }

    public Dataset<Row> getPredictions() {
        return predictions;
    }

    @Override
    public void buildClusterer(ADataSet dataSet, ASettings settings, boolean isPrepared) {
        if (isPrepared) {
            buildCluster(dataSet.getDs(), settings);
        } else {
            buildCluster(dataPrepareClustering.prepareDataSet(dataSet.getDs(), false, removeStrings), settings);
        }
    }

    @Override
    public int clusterRecord(DataRecord dataRecord, boolean isPrepared) {
        Dataset<Row> single;
        if (isPrepared) {
            single = DataPrepare.createDataSet(dataRecord.getRow(), dataRecord.getStructType(), sparkSession);
        } else {
            single = dataPrepareClustering.prepareDataSet(DataPrepare.createDataSet(dataRecord.getRow(), dataRecord.getStructType(), sparkSession), true, removeStrings);
        }
        Dataset<Row> prediction = model.transform(single);
        return (int) prediction.first().get(prediction.schema().fieldIndex(ClusteringStrings.predictionCol));
    }

    @Override
    public Cluster getCluster(int index) {
        Cluster cluster = new Cluster(sparkSession, this.dataPrepareClustering, this.removeStrings);
        cluster.initCluster(predictions.filter(predictions.col(ClusteringStrings.predictionCol).equalTo(index)));
        return cluster;
    }

    @Override
    public int getNoCluster() {
        return model.clusterCenters().length;
    }

    @Override
    public void saveClusterer(String fileName) throws IOException {
        this.predictions.write().mode(SaveMode.Overwrite).json(fileName);
        System.out.println("saveClusterer: " + fileName);
    }

    @Override
    public void loadClusterer(String fileName) throws IOException {
        this.predictions = sparkSession.read().json(fileName);
        System.out.println("loadClusterer: " + fileName);
    }

    private void buildCluster(Dataset<Row> ds, ASettings settings) {


        KMeans km = (KMeans) settings.getModel();
        KMeans kmeans = km
                .setInitMode(org.apache.spark.mllib.clustering.KMeans.RANDOM())
                .setPredictionCol(ClusteringStrings.predictionCol)
                .setFeaturesCol(ClusteringStrings.featuresCol); //normFeatures
        KMeansModel model = kmeans.fit(ds);
        // Make predictions
        Dataset<Row> predictions = model.transform(ds);
        this.kmeans = kmeans;
        this.model = model;
        this.predictions = predictions;

        stringBuilder = stringBuilder
                .append("clusters no. : " + getNoCluster() + "\n")
                .append(getEvaluation())
                .append(getCenters());
    }

    public String getEvaluation() {
        StringBuilder stringBuilderEval = new StringBuilder();
        ClusteringEvaluator clusteringEvaluator = new ClusteringEvaluator();
        clusteringEvaluator.setFeaturesCol(ClusteringStrings.featuresCol);
        clusteringEvaluator.setPredictionCol(ClusteringStrings.predictionCol);
        stringBuilderEval = stringBuilderEval.append("EVAL: " + clusteringEvaluator.evaluate(this.predictions) + "\n");
        ClusteringSummary clusteringSummary = new ClusteringSummary(this.predictions, ClusteringStrings.predictionCol, ClusteringStrings.featuresCol, getNoCluster());
        stringBuilderEval = stringBuilderEval.append("Cluster sizes:\n" + Arrays.toString(clusteringSummary.clusterSizes()) + "\n");
        return stringBuilderEval.toString();
    }

    public String getCenters() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Centers: \n");
        Vector[] centers = model.clusterCenters();
        for (Vector center : centers) {
            stringBuilder.append(center).append("\n");
        }
        return stringBuilder.toString();
    }
}
