package kmeans_implementation.pipeline;


import kmeans_implementation.DataModel;
import kmeans_implementation.Kmns;
import kmeans_implementation.Util;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

/**
 * Created by as on 16.04.2018.
 */
public class KmsEstimator extends Estimator<KmsModel> {

    private static final long serialVersionUID = 5345470610951989479L;
    private String featuresCol = "features";
    private String predictionCol = "prediction";
    private ArrayList<Vector> initialCenters;
    private long seed;
    private double epsilon;
    private int maxIterations;
    private int k;

    public KmsEstimator() {
        this.initialCenters = new ArrayList<>();
        this.seed = 20L;
        this.epsilon =1e-4;
        this.maxIterations = 20;
        this.k = 2;
    }

    public String getFeaturesCol() {
        return featuresCol;
    }

    public KmsEstimator setFeaturesCol(String featuresCol) {
        this.featuresCol = featuresCol;
        return this;
    }

    public String getPredictionCol() {
        return predictionCol;
    }

    public KmsEstimator setPredictionCol(String predictionCol) {
        this.predictionCol = predictionCol;
        return this;
    }

    public ArrayList<Vector> getInitialCenters() {
        return initialCenters;
    }

    public KmsEstimator setInitialCenters(ArrayList<Vector> initialCenters) {
        this.initialCenters = initialCenters;
        return this;
    }

    public long getSeed() {
        return seed;
    }

    public KmsEstimator setSeed(long seed) {
        this.seed = seed;
        return this;
    }

    public double getEpsilon() {
        return epsilon;
    }

    public KmsEstimator setEpsilon(double epsilon) {
        this.epsilon = epsilon;
        return this;
    }

    public int getMaxIterations() {
        return maxIterations;
    }

    public KmsEstimator setMaxIterations(int maxIterations) {
        this.maxIterations = maxIterations;
        return this;
    }

    public int getK() {
        return k;
    }

    public KmsEstimator setK(int k) {
        this.k = k;
        return this;
    }

    @Override
    public KmsModel fit(Dataset<?> dataset) {
        //this.transformSchema(dataset.schema());
        JavaRDD<DataModel> x3 = Util.DatasetToRDD(dataset.select(this.featuresCol));
        if(this.initialCenters.isEmpty()){
            this.initialCenters = Kmns.initializeCenters(x3,this.k);
        }
        ArrayList<Vector> finalCenters = Kmns.computeCenters(x3, initialCenters, this.epsilon, this.maxIterations);
        KmsModel kmsModel = new KmsModel()
                .setClusterCenters(finalCenters)
                .setPredictionCol(this.predictionCol)
                .setFeaturesCol(this.featuresCol);

        return kmsModel;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return structType;
    }

    @Override
    public Estimator<KmsModel> copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public String uid() {
        return "CustomTransformer" + serialVersionUID;
    }
}
