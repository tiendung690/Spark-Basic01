package sparktemplate.clustering;

import org.apache.spark.ml.clustering.KMeans;
import sparktemplate.ASettings2;

/**
 * Created by as on 21.03.2018.
 */
public class ClusteringSettings implements ASettings2 {

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
                System.out.println("Wrong classification type! " + clusteringAlgo);
                return null;
        }
    }

}
