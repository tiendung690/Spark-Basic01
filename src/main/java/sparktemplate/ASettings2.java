package sparktemplate;

import org.apache.spark.ml.classification.*;
import sparktemplate.classifiers.ClassifierName;

/**
 * Created by as on 01.06.2018.
 */
public interface ASettings2 {

    String getAlgo();

    Object getModel();
}
