package sparktemplate.dataprepare;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.feature.OneHotEncoderModel;

/**
 * Klasa pomocnicza.
 *
 * Created by as on 19.03.2018.
 */

public class DataModelsClustering {

    private PipelineModel pipelineModel;
    private OneHotEncoderModel oneHotEncoderModel;

    public DataModelsClustering() {
    }

    public PipelineModel getPipelineModel() {
        return pipelineModel;
    }

    public void setPipelineModel(PipelineModel pipelineModel) {
        this.pipelineModel = pipelineModel;
    }

    public OneHotEncoderModel getOneHotEncoderModel() {
        return oneHotEncoderModel;
    }

    public void setOneHotEncoderModel(OneHotEncoderModel oneHotEncoderModel) {
        this.oneHotEncoderModel = oneHotEncoderModel;
    }

}
