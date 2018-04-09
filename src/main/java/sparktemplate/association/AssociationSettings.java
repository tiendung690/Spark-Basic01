package sparktemplate.association;

import sparktemplate.ASettings;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by as on 15.03.2018.
 */

public class AssociationSettings implements ASettings{
    private double minConfidence;
    private double minSupport;

    public double getMinConfidence() {
        return minConfidence;
    }

    public AssociationSettings setMinConfidence(double minConfidence) {
        this.minConfidence = minConfidence;
        return this;
    }

    public double getMinSupport() {
        return minSupport;
    }

    public AssociationSettings setMinSupport(double minSupport) {
        this.minSupport = minSupport;
        return this;
    }
}