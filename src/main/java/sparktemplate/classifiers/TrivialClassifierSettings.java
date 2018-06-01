package sparktemplate.classifiers;

import org.apache.spark.ml.classification.DecisionTreeClassifier;


//Implementacja zbioru ustawien (opcji) dla klasyfikatora TrivialClassifier

public class TrivialClassifierSettings {
   private ClassifierName classificationAlgo;
   private int maxIter;
   private double regParam;
   private double elasticNetParam;

    public ClassifierName getClassificationAlgo() {
        return classificationAlgo;
    }

    public TrivialClassifierSettings setClassificationAlgo(ClassifierName classificationAlgo) {
        this.classificationAlgo = classificationAlgo;
        return this;
    }

    public int getMaxIter() {
        return maxIter;
    }

    public TrivialClassifierSettings setMaxIter(int maxIter) {
        this.maxIter = maxIter;
        return this;
    }

    public double getRegParam() {
        return regParam;
    }

    public TrivialClassifierSettings setRegParam(double regParam) {
        this.regParam = regParam;
        return this;
    }

    public double getElasticNetParam() {
        return elasticNetParam;
    }

    public TrivialClassifierSettings setElasticNetParam(double elasticNetParam) {
        this.elasticNetParam = elasticNetParam;
        return this;
    }

    public DecisionTree setDecisionTree() {
        this.classificationAlgo = ClassifierName.DECISIONTREE;
        return new DecisionTree();
    }

    public class DecisionTree extends DecisionTreeClassifier {

        @Override
        public DecisionTreeClassifier setMaxDepth(int value) {
            return super.setMaxDepth(value);
        }

        @Override
        public DecisionTreeClassifier setMaxBins(int value) {
            return super.setMaxBins(value);
        }

        public DecisionTree setOK(int iter) {
           maxIter = iter;
           return this;
        }
    }
}

