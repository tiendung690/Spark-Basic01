package sparktemplate.association;

import java.io.IOException;

import sparktemplate.ASettings;
import sparktemplate.datasets.ADataSet;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;


//Interfejs pokazujacy jak implementuje sie metody liczenia regul asocjacyjnych

interface AAssociations {
    
    
    /**
     * Abstrakcyjna metoda szukajaca reguł asocjacyjnych w oparciu o dane.
     *
     * @param dataSet - zbior danych 
     * @param settings ustawienia
     * @param isPrepared dane przygotowane
     */
        
    void buildAssociations(ADataSet dataSet, ASettings settings, boolean isPrepared);


    /**
     * Metoda zapisujaca skupienia do pliku.
     *
     * @param fileName sciezka pliku
     * @throws IOException
     */
    void saveAssociationRules(String fileName) throws IOException;


    /**
     * Metoda zapisujaca skupienia z pliku.
     * @param fileName sciezka pliku
     * @throws IOException
     */

    void loadAssociationRules(String fileName) throws IOException;
 
}
