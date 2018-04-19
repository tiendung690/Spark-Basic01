package myimpl2;


import myimplementation.Kmns;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by as on 16.04.2018.
 */
public class Kms extends Estimator<KmsModel> {

    private static final long serialVersionUID = 5345470610951989479L;
    private String featuresCol = "features";
    private String predictionCol = "prediction";
    private long seed = 20L;
    private int maxIter = 20;
    private int k = 2;

    public Kms setFeaturesCol(String featuresCol) {
        this.featuresCol = featuresCol;
        return this;
    }

    public Kms setPredictionCol(String predictionCol) {
        this.predictionCol = predictionCol;
        return this;
    }

    public Kms setSeed(long seed) {
        this.seed = seed;
        return this;
    }

    public Kms setMaxIter(int maxIter) {
        this.maxIter = maxIter;
        return this;
    }

    public Kms setK(int k) {
        this.k = k;
        return this;
    }

    public String getFeaturesCol() {
        return featuresCol;
    }

    public String getPredictionCol() {
        return predictionCol;
    }

    public long getSeed() {
        return seed;
    }

    public int getMaxIter() {
        return maxIter;
    }

    public int getK() {
        return k;
    }

    @Override
    public KmsModel fit(Dataset<?> dataset) {
        this.transformSchema(dataset.schema());

        JavaRDD<Vector> x3 = Kmns.convertToRDD((Dataset<Row>) dataset.select(this.featuresCol));
        x3.cache();
        ArrayList<Vector> clusterCenters = new ArrayList<>(x3.takeSample(false, this.k, this.seed));
        ArrayList<Vector> clusterCenters2 = Kmns.computeCenters(x3, clusterCenters);
        //String s = dataset.toJavaRDD().take(1).get(0).toString();
        KmsModel kmsModel = new KmsModel()
                .setClusterCenters(clusterCenters2)
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
