package sparktemplate.association;

import org.apache.spark.ml.fpm.FPGrowth;
import sparktemplate.ASettings;
import sparktemplate.ASettings2;
import sparktemplate.classifiers.ClassifierName;
import sparktemplate.classifiers.ClassifierSettings;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by as on 15.03.2018.
 */

public class AssociationSettings implements ASettings2 {

    private FPGrowth fpGrowth;
    private AssociationName associationAlgo; //wybrany algorytm

    public class AssociationFP extends FPGrowth{}

    public AssociationFP setFPGrowth() {
        associationAlgo = AssociationName.fpgrowth;
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
            case fpgrowth: {
                return fpGrowth;
            }
            default:
                System.out.println("Wrong classification type! " + associationAlgo);
                return null;
        }
    }

    public static void main(String[] args) {
        AssociationSettings as = new AssociationSettings();
        FPGrowth ff = (FPGrowth) as.getModel();
    }
}