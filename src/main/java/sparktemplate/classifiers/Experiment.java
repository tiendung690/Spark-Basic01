package sparktemplate.classifiers;

import sparktemplate.datasets.MemDataSet;

public class Experiment {

public static void main(String[] args) 
    {
        try 
        {
//            String fNameTabTrain = "C:/DANE/train_data.csv"; //Okreslenie lokalizacji pliku z danymi treningowymi
//            String fNameTabTest = "C:/DANE/test_data.csv"; //Okreslenie lokalizacji pliku z danymi testowymi
//
//            MemDataSet dataSetTrain = new MemDataSet(); //Utworzenie obiektu na dane treningowe
//            dataSetTrain.loadDataSet(fNameTabTrain); //Wczytanie danych treningowych
//
//
//            //Utworzenie obiektu opcji do tworzenia klasyfikatora
//            TrivialClassifierSettings classifierSettings = new TrivialClassifierSettings(1,"Ala");
//
//            MemDataSet dataSetTest = new MemDataSet(); //Utworzenie obiektu na dane testowe
//            dataSetTest.loadDataSet(fNameTabTest); //Wczytanie danych testowych
//
//            //Utworzenie obiektu testowania roznymi metodami
//            Evaluation evaluation = new Evaluation();
//
//            //Wywolanie metody testujacej metoda Train&Test
//            evaluation.makeTrainAndTest(dataSetTrain,dataSetTest,classifierSettings);
//
//            evaluation.printReport();

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Done.");
    }    
    
}
