package sparktemplate.classifiers;

import sparktemplate.ASettings;

import java.util.HashMap;
import java.util.Map;


//Implementacja zbioru ustawien (opcji) dla klasyfikatora TrivialClassifier

public class TrivialClassifierSettings implements ASettings
{
   private String classificationAlgo;
   private int maxIter;
   private double regParam;
   private double elasticNetParam;

    public String getClassificationAlgo() {
        return classificationAlgo;
    }

    public TrivialClassifierSettings setClassificationAlgo(String classificationAlgo) {
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
}
