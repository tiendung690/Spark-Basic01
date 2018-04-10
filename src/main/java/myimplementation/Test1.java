package myimplementation;

import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Created by as on 10.04.2018.
 */
public class Test1 extends Transformer {
    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        return null;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return null;
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return null;
    }

    @Override
    public String uid() {
        return null;
    }
}

class Test2 extends Estimator{

    @Override
    public Model fit(Dataset dataset) {
        return null;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return null;
    }

    @Override
    public Estimator copy(ParamMap paramMap) {
        return null;
    }

    @Override
    public String uid() {
        return null;
    }
}
