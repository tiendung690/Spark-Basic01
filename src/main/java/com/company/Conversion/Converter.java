package com.company.Conversion;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by as on 03.01.2018.
 */
public class Converter {

    public static void main(String[] args) {
        String headers_path = "data/mllib/kdd_headers.txt";
        String data_path = "data/mllib/kdd_10_proc.txt";
        Converter.convert(headers_path, data_path);
    }

    /*
    Metoda konwertująca dane w formacie csv na kolekcję JavaRDD<LabeledPoint>.
    headers_path - ścieżka do pliku z nagłówkami
    data_path - ścieżka do pliku z danymi
     */
    public static JavaRDD<LabeledPoint> convert(String headers_path, String data_path) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("JavaWordCount")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);


        //ETAP 1 ***********************************
        //Konwersja danych symbolicznych na numeryczne.

        //Wczytanie nagłówków

        Dataset<Row> headers = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .load(headers_path);

        String[] h_cols = headers.columns();


        //Wczytanie danych

        Dataset<Row> df = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", true) // Stare nagłówki zostaną nadpisane
                .option("inferSchema", true) // Automatycznie ustala typy kolumn
                .load(data_path)
                .toDF(h_cols); // Załadowanie nowych nagłówków



        /*
        *  Utworzenie obiektów StringIndexer dla wybranych kolumn z wartościami symbolicznymi.
        *  StringIndexer zamienia wartości symboliczne na numeryczne
        *   id | category | categoryIndex
            ----|----------|---------------
             0  | a        | 0.0
             1  | b        | 2.0
             2  | c        | 1.0
             3  | a        | 0.0
             4  | a        | 0.0
             5  | c        | 1.0
         */

        StringIndexer indexerProtocol = new StringIndexer()
                .setInputCol("protocol_type")
                .setOutputCol("protocol_type_index");

        StringIndexer indexerService = new StringIndexer()
                .setInputCol("service")
                .setOutputCol("service_index");

        StringIndexer indexerFlag = new StringIndexer()
                .setInputCol("flag")
                .setOutputCol("flag_index");

        StringIndexer indexerClass = new StringIndexer()
                .setInputCol("class")
                .setOutputCol("class_index");


        // Zastosowanie Pipeline w celu uruchomienia sekwencji kilku operacji na danych

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{indexerProtocol, indexerService, indexerFlag, indexerClass});
        PipelineModel model = pipeline.fit(df);

        // Wczytanie danych po transformacji

        Dataset<Row> dfA = model.transform(df);

        // Wyświetlenie kolumn po transformacji

        dfA.printSchema();

        // Podmiana wartości kolumn, usunięcie niepotrzebnych kolumn.

        Dataset<Row> dfB = dfA
                .withColumn("protocol_type", dfA.col("protocol_type_index"))
                .withColumn("service", dfA.col("service_index"))
                .withColumn("flag", dfA.col("flag_index"))
                .withColumn("class", dfA.col("class_index"))
                .drop("protocol_type_index", "service_index", "flag_index", "class_index");

        System.out.println("AFTER CONVERSION");

        // Wypisanie struktury i 2 obiekty.

        dfB.printSchema();
        dfB.show(2);


        // Wypisanie przypisanych wartości dla każdej klasy

        System.out.println("--------------------------LABELS-------------------------------");

        dfA.select("protocol_type", "protocol_type_index").distinct().orderBy("protocol_type_index").show();
        dfA.select("service", "service_index").distinct().orderBy("service_index").show();
        dfA.select("flag", "flag_index").distinct().orderBy("flag_index").show();
        dfA.select("class", "class_index").distinct().orderBy("class_index").show();
        dfA.select("protocol_type", "protocol_type_index", "service", "service_index", "flag", "flag_index", "class", "class_index").show(10);


        //Etap 2 ***********************************
        //Konwersja DataSet na docelowy JavaRDD<LabeledPoint>

        JavaRDD<Row> data = dfB.toJavaRDD();
        JavaRDD<LabeledPoint> ldat = data
                .map(row -> {
                    // Pobranie klasy decyzyjnej (ostatnie miejsce)
                    double d = row.getDouble(row.size() - 1);
                    // Wczytanie danych do tablicy
                    double[] features = new double[row.size() - 1];
                    for (int i = 0; i < row.size() - 1; i++) {
                        features[i] = Double.parseDouble(row.get(i).toString());
                    }
                    // SPARSE/DENSE .toSparse()
                    // Zwrócenie obiektu LabeledPoint
                    return new LabeledPoint(d, org.apache.spark.mllib.linalg.Vectors.dense(features).toSparse());
                });

        //Zapis do pliku

        //ldat.saveAsTextFile("KDDCUP");

        //Wypisanie 2 obiektów LabeledPoint

        System.out.println("LABELED POINT #  " + ldat.take(2).toString());

        return ldat;
    }
}
