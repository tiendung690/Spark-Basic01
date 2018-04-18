package myimpl2;


import myimplementation.Kmns;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.lit;

/**
 * Created by as on 16.04.2018.
 */
public class KmsModel extends Model<KmsModel> {

    private static final long serialVersionUID = 5542470640921989462L;
    private String text;
    private ArrayList<Vector> clusterCenters;

    public void setClusterCenters(ArrayList<Vector> clusterCenters) {
        this.clusterCenters = clusterCenters;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        JavaRDD<Vector> x3 = Kmns.convertToRDD((Dataset<Row>) dataset);
        JavaRDD<Kmns.DataModel> x5 = Kmns.computeDistancesAndPredictCluster(x3, this.clusterCenters);
        Dataset<Row> dm = Kmns.createDataSetUDF(x5, SparkSession.getActiveSession().get());
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
