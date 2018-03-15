package sparktemplate.classifiers;

import java.io.IOException;
import sparktemplate.ASettings;
import sparktemplate.DataRecord;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;


/**
 * Klasa <tt>TrivialClassifier</tt> demonstruje jak sie implementuje klasyfikatory
 *
 * @author Jan G. Bazan
 * @version 1.0, luty 2018 roku
 */


public class TrivialClassifier implements AClassifier
{
    //Tutaj struktury danych zwiazane z klasyfikatorem
    
        
    //Trywialna implementacja tworzenia klasyfikatora
    public void build(MemDataSet dataSet, ASettings settings) 
    {
        //Odczytanie parametrow
        TrivialClassifierSettings  locSettings = (TrivialClassifierSettings)settings;        
        System.out.println("Pierwszy parametr="+locSettings.getParameter1());
        System.out.println("Drugi parametr="+locSettings.getParameter2());        
        
        //Tutaj tworzenie klasyfikatora
        
    }
    
    public void build(DBDataSet dataSet, ASettings settings)
    {
        //Brak implementacji
    }
        
    //Trywialna implementacja testowania klasyfikatora na obiekcie testowym
    public String classify(DataRecord dataRecord)
    {
        return "NO";
    }
    

    public void saveClassifier(String fileName) throws IOException {}

    public void loadClassifier(String fileName) throws IOException {}
   
    
}
