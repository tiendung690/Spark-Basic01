package sparktemplate.clustering;

import java.io.IOException;
import sparktemplate.ASettings;
import sparktemplate.DataRecord;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;


//Interfejs pokazujacy jak implementuje sie metody grupowania

interface AClustering {
    
   
    
     /**
     * Abstrakcyjna metoda szukajaca skupien w oparciu o dane z obiektu klasy MemDataSet.
     *
     * @param dataSet - zbior danych 
     * @param settings ustawienia 
     */
        
    void buildClusterer(MemDataSet dataSet, ASettings settings);
    void buildClusterer(MemDataSet dataSet);
    
    /**
     * Abstrakcyjna metoda szukajaca skupien w oparciu o dane z obiektu klasy DBDataSet.
     *
     * @param dataSet - zbior danych
     * @param settings ustawienia 
     */
        
    void buildClusterer(DBDataSet dataSet, ASettings settings);
    void buildClusterer(DBDataSet dataSet);
    
    /**
     * Abstrakcyjna metoda testujaca rekord na przynaleznosc do skupienia
     * 
     * @param dataRecord - rekord testowy
     * @return numer skupienia (numeracja od 0 do liczba skupien-1)     
     */

    int clusterRecord(DataRecord dataRecord);
        
    
    //Pobiera skupienie o podanym numerze
    Cluster getCluster(int index); 
    
    
    //udostepnia liczbe skupien
    int getNoCluster(); 
    

    /**
     * Zapis do pliku tekstowego o podanej nazwie
     * @param fileName nazwa pliku
     * @throws IOException
     */

    void saveClusterer(String fileName) throws IOException;


    /**
     * Odczyt z pliku tekstowego o podanej nazwie
     * @param fileName nazwa pliku
     * @throws IOException
     */

    void loadClusterer(String fileName) throws IOException;
    
 
}
