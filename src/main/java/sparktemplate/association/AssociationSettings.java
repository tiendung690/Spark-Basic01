package sparktemplate.association;

import org.apache.spark.ml.fpm.FPGrowth;
import sparktemplate.ASettings;

/**
 * Created by as on 15.03.2018.
 */

public class AssociationSettings implements ASettings {

    private FPGrowth fpGrowth;
    private AssociationName associationAlgo; //wybrany algorytm

    public class AssociationFP extends FPGrowth{}

    public AssociationFP setFPGrowth() {
        associationAlgo = AssociationName.FPGROWTH;
        fpGrowth = new AssociationFP();
        return (AssociationFP) fpGrowth;
    }

    @Override
    public String getAlgo() {
        return associationAlgo.toString();
    }

    @Override
    public Object getModel() {
        switch (associationAlgo) {
            case FPGROWTH: {
                return fpGrowth;
            }
            default:
                System.out.println("Wrong type! " + associationAlgo);
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