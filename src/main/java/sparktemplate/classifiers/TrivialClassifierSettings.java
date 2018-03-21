package sparktemplate.classifiers;

import sparktemplate.ASettings;

import java.util.HashMap;
import java.util.Map;


//Implementacja zbioru ustawien (opcji) dla klasyfikatora TrivialClassifier

public class TrivialClassifierSettings implements ASettings
{

    private int parameter1;
    private String parameter2;

    public TrivialClassifierSettings(int parameter1, String parameter2) {
        this.parameter1 = parameter1;
        this.parameter2 = parameter2;
    }

    public int getParameter1() {
        return parameter1;
    }

    public void setParameter1(int parameter1) {
        this.parameter1 = parameter1;
    }

    public String getParameter2() {
        return parameter2;
    }

    public void setParameter2(String parameter2) {
        this.parameter2 = parameter2;
    }


    @Override
    public Map getMap() {
        Map<String, String> map = new HashMap();
        map.put("type", getParameter2());
        return map;
    }
}
