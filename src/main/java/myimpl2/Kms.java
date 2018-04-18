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

    @Override
    public KmsModel fit(Dataset<?> dataset) {
        this.transformSchema(dataset.schema());

        JavaRDD<Vector> x3 = Kmns.convertToRDD((Dataset<Row>) dataset);
        x3.cache();
        ArrayList<Vector> clusterCenters = new ArrayList<>(x3.takeSample(false, 3, 20L));
        ArrayList<Vector> clusterCenters2 = Kmns.computeCenters(x3, clusterCenters);
        //String s = dataset.toJavaRDD().take(1).get(0).toString();
        KmsModel kmsModel = new KmsModel();
        kmsModel.setClusterCenters(clusterCenters2);
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
