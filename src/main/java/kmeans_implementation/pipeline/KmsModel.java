package kmeans_implementation.pipeline;


import kmeans_implementation.DataModel;
import kmeans_implementation.Kmns;
import kmeans_implementation.Util;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

/**
 * Created by as on 16.04.2018.
 */
public class KmsModel extends Model<KmsModel> {

    private static final long serialVersionUID = 5542470640921989462L;
    private ArrayList<Vector> clusterCenters;
    private String featuresCol = "features";
    private String predictionCol = "prediction";

    public KmsModel setClusterCenters(ArrayList<Vector> clusterCenters) {
        this.clusterCenters = clusterCenters;
        return this;
    }

    public KmsModel setFeaturesCol(String featuresCol) {
        this.featuresCol = featuresCol;
        return this;
    }

    public KmsModel setPredictionCol(String predictionCol) {
        this.predictionCol = predictionCol;
        return this;
    }

    public ArrayList<Vector> getClusterCenters() {
        return clusterCenters;
    }

    public String getFeaturesCol() {
        return featuresCol;
    }

    public String getPredictionCol() {
        return predictionCol;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {

        JavaRDD<DataModel> x3 = Util.DatasetToRDD(dataset.select(this.featuresCol));
        JavaPairRDD<Integer, Vector> x5 = Kmns.predictCluster2(x3, this.clusterCenters);
        Dataset<Row> dm = Util.RDDToDataset(x5, SparkSession.getActiveSession().get(), this.featuresCol, this.predictionCol);
        return dm;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return structType;
    }

    @Override
    public KmsModel copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public String uid() {
        return "CustomTransformer" + serialVersionUID;
    }
}
