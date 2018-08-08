package sparktemplate.clustering;

import java.io.IOException;

import sparktemplate.ASettings;
import sparktemplate.DataRecord;
import sparktemplate.datasets.ADataSet;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;


//Interfejs pokazujacy jak implementuje sie metody grupowania

interface AClustering {

    /**
     * Abstrakcyjna metoda szukajaca skupien w oparciu o dane.
     *
     * @param dataSet    - zbior danych
     * @param settings   - ustawienia
     * @param isPrepared - dane przygotowane
     */
    void buildClusterer(ADataSet dataSet, ASettings settings, boolean isPrepared);


    /**
     * Abstrakcyjna metoda testujaca rekord na przynaleznosc do skupienia
     *
     * @param dataRecord - rekord testowy
     * @param isPrepared - dane przygotowane
     * @return numer skupienia (numeracja od 0 do liczba skupien-1)
     */
    int clusterRecord(DataRecord dataRecord, boolean isPrepared);


    /**
     * Metoda zwracajaca skupienie o podanym numerze
     *
     * @param index numer skupienia
     * @return skupienie, klaster
     */
    Cluster getCluster(int index);


    /**
     * Metoda zwracajaca liczbe skupien
     *
     * @return
     */
    int getNoCluster();


    /**
     * Zapis do pliku tekstowego o podanej nazwie
     *
     * @param fileName nazwa pliku
     * @throws IOException
     */

    void saveClusterer(String fileName) throws IOException;


    /**
     * Odczyt z pliku tekstowego o podanej nazwie
     *
     * @param fileName nazwa pliku
     * @throws IOException
     */

    void loadClusterer(String fileName) throws IOException;


}
