package sparktemplate.classifiers;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import sparktemplate.ASettings;
import sparktemplate.datarecord.DataRecord;
import sparktemplate.datasets.ADataSet;


/**
 * Interfejs  <tt>AClassifier</tt> opisuje podstawowe funkcjonalnosci zwiazane z
 * klasyfikatorami budowanymi w oparciu o dane z pamieci i z bazy danych
 *
 * @author Jan G. Bazan
 * @version 1.0, luty 2018 roku
 */

public interface AClassifier {

    /**
     * Abstrakcyjna metoda budujaca klasyfikator w oparciu o dane z obiektu klasy DBDataSet.
     *
     * @param dataSet    - zbior danych dla ktorego budowany jest klasyfikator
     * @param isPrepared - dane przygotowane
     * @param settings   ustawienia klasyfikatora (dla każdej metody konstrukcji klasyfikatora implementujemy tę klasę inaczej)
     */
    void build(ADataSet dataSet, ASettings settings, boolean isPrepared, boolean removeStrings);


    /**
     * Abstrakcyjna metoda testujaca rekordy na przynaleznosc do klas decyzyjnych.
     *
     * @param dbDataSet  - dane
     * @param aSettings  - ustawienia
     * @param isPrepared - dane przygotowane
     * @return
     */
    Dataset<Row> classify(ADataSet dbDataSet, ASettings aSettings, boolean isPrepared, boolean removeStrings);

    /**
     * Abstrakcyjna metoda testujaca rekord na przynaleznosc do klas decyzyjnych.
     *
     * @param dataRecord - rekord testowy
     * @param aSettings  - ustawienia
     * @param isPrepared - dane przygotowane
     * @return nazwa klasy decyzyjne, do ktorej rekord zostal sklasyfikowany (wartośc typu String)
     */
    String classify(DataRecord dataRecord, ASettings aSettings, boolean isPrepared, boolean removeStrings);


    /**
     * Zapis klasyfikatora do pliku tekstowego o podanej nazwie
     *
     * @param fileName nazwa pliku
     * @throws IOException
     */
    void saveClassifier(String fileName) throws IOException;


    /**
     * Odczyt klasyfikatora z pliku tekstowego o podanej nazwie
     *
     * @param fileName nazwa pliku
     * @throws IOException
     */
    void loadClassifier(String fileName) throws IOException;


}
