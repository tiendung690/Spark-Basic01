package sparktemplate.clustering;

import sparktemplate.ASettings;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by as on 21.03.2018.
 */
public class ClusteringSettings implements ASettings {

    private int k;
    private long seed;

    public int getK() {
        return k;
    }

    public ClusteringSettings setK(int k) {
        this.k = k;
        return this;
    }

    public long getSeed() {
        return seed;
    }

    public ClusteringSettings setSeed(long seed) {
        this.seed = seed;
        return this;
    }

}
