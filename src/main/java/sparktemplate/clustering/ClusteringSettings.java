package sparktemplate.clustering;

import org.apache.spark.ml.clustering.KMeans;
import sparktemplate.ASettings;

/**
 * Created by as on 21.03.2018.
 */
public class ClusteringSettings implements ASettings {

    private KMeans kMeans;
    private ClusteringName clusteringAlgo; //wybrany algorytm

    public class ClusteringKMeans extends KMeans{}

    public ClusteringKMeans setKMeans() {
        clusteringAlgo = ClusteringName.KMEANS;
        kMeans = new ClusteringKMeans();
        return (ClusteringKMeans) kMeans;
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
