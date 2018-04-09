package sparktemplate.association;

import java.io.IOException;
import sparktemplate.ASettings;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;


//Interfejs pokazujacy jak implementuje sie metody liczenia regul asocjacyjnych

interface AAssociations {
    
    
    /**
     * Abstrakcyjna metoda szukajaca reguł asocjacyjnych w oparciu o dane z obiektu klasy MemDataSet.
     *
     * @param dataSet - zbior danych 
     * @param settings ustawienia 
     */
        
    void buildAssociations(MemDataSet dataSet, ASettings settings);
    //void buildAssociations(MemDataSet dataSet);
    
    /**
     * Abstrakcyjna metoda szukajaca regul asocjacyjnych  w oparciu o dane z obiektu klasy DBDataSet.
     *
     * @param dataSet - zbior danych 
     * @param settings ustawienia 
     */
        
    void buildAssociations(DBDataSet dataSet, ASettings settings);
    //void buildAssociations(DBDataSet dataSet);
    /**
     * Zapis do pliku tekstowego o podanej nazwie
     * @param fileName nazwa pliku
     * @throws IOException
     */

    void saveAssociationRules(String fileName) throws IOException;


    /**
     * Odczyt z pliku tekstowego o podanej nazwie
     * @param fileName nazwa pliku
     * @throws IOException
     */

    void loadAssociationRules(String fileName) throws IOException;
 
}
