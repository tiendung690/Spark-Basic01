package sparktemplate.clustering;

import kmeansimplementation.pipeline.KMeansImplEstimator;
import org.apache.spark.ml.clustering.KMeans;
import sparktemplate.ASettings;

/**
 * Created by as on 21.03.2018.
 */
public class ClusteringSettings implements ASettings {

    private KMeans kMeans;
    private KMeansImplEstimator kMeansImplEstimator;
    private ClusteringName clusteringAlgo;

    public class ClusteringKMeans extends KMeans{}
    public class ClusteringKMeansImpl extends KMeansImplEstimator {}

    public ClusteringKMeans setKMeans() {
        clusteringAlgo = ClusteringName.KMEANS;
        kMeans = new ClusteringKMeans();
        return (ClusteringKMeans) kMeans;
    }

    public ClusteringKMeansImpl setKMeansImpl(){
        clusteringAlgo = ClusteringName.KMEANSIMPL;
        kMeansImplEstimator = new ClusteringKMeansImpl();
        return (ClusteringKMeansImpl) kMeansImplEstimator;
    }

    @Override
    public String getAlgo() {
        return clusteringAlgo.toString();
    }

    @Override
    public Object getModel() {
        switch (clusteringAlgo) {
            case KMEANS: {
                return kMeans;
            }
            case KMEANSIMPL: {
                return kMeansImplEstimator;
            }
            default:
                System.out.println("Wrong type! " + clusteringAlgo);
                return null;
        }
    }

    @Override
    public Object setLabelName(String labelName) {
        return null;
    }

    @Override
    public String getLabelName() {
        return null;
    }

}
