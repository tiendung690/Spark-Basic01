package sparktemplate.classifiers;

import org.apache.spark.ml.classification.*;
import sparktemplate.ASettings2;

/**
 * Created by as on 01.06.2018.
 */
public class ClassifierSettings implements ASettings2 {

    private DecisionTreeClassifier decisionTreeClassifier;
    private RandomForestClassifier randomForestClassifier;
    private LinearSVC linearSVC;
    private LogisticRegression logisticRegression;
    private NaiveBayes naiveBayes;
    private ClassifierName classificationAlgo; //wybrany algorytm

    public class ClassifierDecisionTree extends DecisionTreeClassifier {}

    public class ClassifierRandomForest extends RandomForestClassifier {}

    public class ClassifierLinearSVC extends LinearSVC {}

    public class ClassifierLogisticRegression extends LogisticRegression {}

    public class ClassifierNaiveBayes extends NaiveBayes {}

    public ClassifierDecisionTree setDecisionTree() {
        classificationAlgo = ClassifierName.decisiontree;
        //ClassifierDecisionTree cc = new ClassifierDecisionTree();
        //decisionTreeClassifier = cc;
        //return cc;
        decisionTreeClassifier = new ClassifierDecisionTree(); //
        return (ClassifierDecisionTree) decisionTreeClassifier; //
    }

    public ClassifierRandomForest setRandomForest() {
        classificationAlgo = ClassifierName.randomforests;
        randomForestClassifier = new ClassifierRandomForest();
        return (ClassifierRandomForest) randomForestClassifier;
    }

    public ClassifierLinearSVC setLinearRegression() {
        classificationAlgo = ClassifierName.linearsvm;
        linearSVC = new ClassifierLinearSVC();
        return (ClassifierLinearSVC) linearSVC;
    }

    public ClassifierLogisticRegression setLogisticRegression() {
        classificationAlgo = ClassifierName.logisticregression;
        logisticRegression = new ClassifierLogisticRegression();
        return (ClassifierLogisticRegression) logisticRegression;
    }

    public ClassifierNaiveBayes setNaiveBayes() {
        classificationAlgo = ClassifierName.naivebayes;
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
            case linearsvm: {
                return linearSVC;
            }
            case decisiontree: {
                return decisionTreeClassifier;
            }
            case randomforests: {
                return randomForestClassifier;
            }
            case logisticregression: {
                return logisticRegression;
            }
            case naivebayes: {
                return naiveBayes;
            }
            default:
                System.out.println("Wrong classification type! " + classificationAlgo);
                return null;
        }
    }
}
