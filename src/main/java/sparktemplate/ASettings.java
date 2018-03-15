package sparktemplate;

/**
 * Intefejs  <tt>ASettings</tt> reprezentuje abstrakcyjne funkcjonalnosci
 * obiektow z parametrami do metod eksploracji danych
 *
 * @author Jan G. Bazan
 * @version 1.0, luty 2018 roku
 */
public interface ASettings<T> {

    //Klasy implementujace ten interfejs są definiowane na potrzeby reprezentowania parametrow poszczególnych metod eksploracji danych

    boolean hasKey(String key);

    String getValue(String key);

    T setting(String key, String value);

}
