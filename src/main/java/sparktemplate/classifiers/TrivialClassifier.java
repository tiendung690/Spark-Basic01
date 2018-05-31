package sparktemplate.classifiers;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import sparktemplate.ASettings;
import sparktemplate.DataRecord;
import sparktemplate.datasets.ADataSet;
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
        System.out.println("Pierwszy parametr="+locSettings.getClassificationAlgo());
        System.out.println("Drugi parametr="+locSettings.getMaxIter());
        
        //Tutaj tworzenie klasyfikatora
        
    }
    
    public void build(DBDataSet dataSet, ASettings settings)
    {
        //Brak implementacji
    }

    @Override
    public void build(ADataSet dataSet, ASettings settings) {

    }

    @Override
    public Dataset<Row> makePredictions(ADataSet dbDataSet) {
        return null;
    }

    //Trywialna implementacja testowania klasyfikatora na obiekcie testowym
    public String classify(DataRecord dataRecord)
    {
        return "NO";
    }
    

    public void saveClassifier(String fileName) throws IOException {}

    public void loadClassifier(String fileName) throws IOException {}
   
    
}
