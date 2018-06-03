package sparktemplate.classifiers;

import org.apache.spark.ml.classification.*;
import sparktemplate.ASettings;

/**
 * Created by as on 01.06.2018.
 */
public class ClassifierSettings implements ASettings {

    private DecisionTreeClassifier decisionTreeClassifier;
    private RandomForestClassifier randomForestClassifier;
    private LinearSVC linearSVC;
    private LogisticRegression logisticRegression;
    private NaiveBayes naiveBayes;
    private ClassifierName classificationAlgo; //wybrany algorytm
    private String labelName; // nazwa kolumny klasy decyzyjnej

    public ClassifierSettings setLabelName(String labelName) {this.labelName = labelName; return this;}

    public String getLabelName() {return labelName;}

    public class ClassifierDecisionTree extends DecisionTreeClassifier {}

    public class ClassifierRandomForest extends RandomForestClassifier {}

    public class ClassifierLinearSVC extends LinearSVC {}

    public class ClassifierLogisticRegression extends LogisticRegression {}

    public class ClassifierNaiveBayes extends NaiveBayes {}

    public ClassifierDecisionTree setDecisionTree() {
        classificationAlgo = ClassifierName.DECISIONTREE;
        //ClassifierDecisionTree cc = new ClassifierDecisionTree();
        //decisionTreeClassifier = cc;
        //return cc;
        decisionTreeClassifier = new ClassifierDecisionTree(); //
        return (ClassifierDecisionTree) decisionTreeClassifier; //
    }

    public ClassifierRandomForest setRandomForest() {
        classificationAlgo = ClassifierName.RANDOMFORESTS;
        randomForestClassifier = new ClassifierRandomForest();
        return (ClassifierRandomForest) randomForestClassifier;
    }

    public ClassifierLinearSVC setLinearRegression() {
        classificationAlgo = ClassifierName.LINEARSVM;
        linearSVC = new ClassifierLinearSVC();
        return (ClassifierLinearSVC) linearSVC;
    }

    public ClassifierLogisticRegression setLogisticRegression() {
        classificationAlgo = ClassifierName.LOGISTICREGRESSION;
        logisticRegression = new ClassifierLogisticRegression();
        return (ClassifierLogisticRegression) logisticRegression;
    }

    public ClassifierNaiveBayes setNaiveBayes() {
        classificationAlgo = ClassifierName.NAIVEBAYES;
        naiveBayes = new ClassifierNaiveBayes();
        return (ClassifierNaiveBayes) naiveBayes;
    }


    @Override
    public String getAlgo() {
        return classificationAlgo.toString();
    }


    @Override
    public Object getModel() {

        switch (classificationAlgo) {
            case LINEARSVM: {
                return linearSVC;
            }
            case DECISIONTREE: {
                return decisionTreeClassifier;
            }
            case RANDOMFORESTS: {
                return randomForestClassifier;
            }
            case LOGISTICREGRESSION: {
                return logisticRegression;
            }
            case NAIVEBAYES: {
                return naiveBayes;
            }
            default:
                System.out.println("Wrong type! " + classificationAlgo);
                return null;
        }
    }
}
