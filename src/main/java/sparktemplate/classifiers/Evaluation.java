package sparktemplate.classifiers;

import sparktemplate.ASettings;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;


/**
 * Klasa  <tt>Evaluation</tt> opisuje standardowe funkcjonalnosci obiektu
 * sluzacego do testowania klasyfikatorów
 *
 * @author Jan G. Bazan
 * @version 1.0, luty 2018 roku
 */

public class Evaluation {

    /**
     * Metoda zwracajaca accuracy wykonanego wczesniej eksperymentu
     *
     * @return Wartosc accuracy
     */
    
    double getAccuracy() { return 1.0; }

    /**
     * Metoda zwracajaca accuracy dla podanej klasy decyzyjnej, przy czym
     * wartosc decyzji podana jest w formie tekstowej
     *
     * @param decValue Klasa decyzyjna w formie tekstowej
     * @return Wartosc accuracy dla podanej klasy decyzyjnej
     */
    public double getAccuracy(String decValue) { return 1.0; }

    
    
    /**
     * Metoda zwracajaca coverage wykonanego wczesniej eksperymentu
     *
     * @return Wartosc coverage
     */
    double getCoverage() { return 1.0; }

    
    /**
     * Metoda zwracajaca coverage dla podanej klasy decyzyjnej, przy czym
     * wartosc decyzji podana jest w formie tekstowej
     *
     * @param decValue Klasa decyzyjna w formie tekstowej
     * @return Wartosc coverage dla podanej klasy decyzyjnej
     */
    double getCoverage(String decValue) { return 1.0; }

    
    /**
     * Cztery warianty metodTraiAndTest dla zbiorów danych
     *
     * @param trainingDataSet - zbior danych treningowych
     * @param testingDataSet - zbior danych testowych
     * @param settings - obiekt parametrow     
     */
    
    void makeTrainAndTest(MemDataSet trainingDataSet, MemDataSet testingDataSet, ASettings classifierSettings)  {}  //Wykonywaniu testu        
    void makeTrainAndTest(MemDataSet trainingDataSet, DBDataSet testingDataSet, ASettings classifierSettings)  {}  //Wykonywaniu testu        
    void makeTrainAndTest(DBDataSet trainingDataSet, DBDataSet testingDataSet, ASettings classifierSettings)  {}  //Wykonywaniu testu        
    void makeTrainAndTest(DBDataSet trainingDataSet, MemDataSet testingDataSet, ASettings classifierSettings)  {}  //Wykonywaniu testu        
    
       
    /**
     * Metoda wypisuje na ekran tekst opisujacy wyniki ekperymentu
     *
     */
    public void printReport(){ System.out.println("Wyniki:"); };
    

}
