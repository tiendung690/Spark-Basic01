package sparktemplate;

/**
 * Intefejs reprezentuje abstrakcyjne funkcjonalnosci obiektow z parametrami do metod eksploracji danych
 *
 * @author Jan G. Bazan
 * @version 1.0, luty 2018 roku
 */

public interface ASettings {

    //Klasy implementujace ten interfejs są definiowane na potrzeby reprezentowania parametrow poszczególnych metod eksploracji danych

    String getAlgo();

    Object getModel();

    Object setLabelName(String labelName);

    String getLabelName();
}
