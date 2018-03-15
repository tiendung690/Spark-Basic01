package com.company.Other;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.Arrays;
import java.util.function.Function;

/**
 * Created by as on 30.12.2017.
 */
public class Main5Conversion {
    public static void main(String[] args) {

        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        // $example on$
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("JavaDecisionTreeClassificationExample");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);


////        String path = "C:/Users/as/IdeaProjects/Converter/kddcup_train.txt";
//        String path = "data/mllib/convert_csv.txt";
//        JavaRDD<String> data = jsc.textFile(path);
//        JavaRDD<LabeledPoint> parsedData = data
//                .map( line -> {
//                    String[] parts = line.split(",");
//                    return new LabeledPoint(Double.parseDouble(parts[0]),
//                            Vectors.dense(Double.parseDouble(parts[1]),
//                                    Double.parseDouble(parts[2]),
//                                    Double.parseDouble(parts[3])));
//                })
//                .map( line -> new LabeledPoint(line.label(),
//                        Vectors.dense(line.features().apply(0),
//                                line.features().apply(1),
//                                line.features().apply(2)))
//                );
//
//
//        parsedData.foreach(f -> System.out.println(f));
////        System.out.println(parsedData.first().toString());
//

        //        String path = "C:/Users/as/IdeaProjects/Converter/kddcup_train.txt";
        String path = "data/mllib/convert_csv.txt";
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<LabeledPoint> parsedData = data
                .map( line -> {
                    String[] parts = line.split(",");
                    return parts;
                })
                .map( line -> {
                    String[] parts = new String[line.length];
                    for (int i = 0; i < line.length ; i++) {
                        if(isNumerical(line[i])){
                            parts[i]=line[i];
                        }else{
                            parts[i]="1";
                        }
                    }
                    return parts;
                })
                .map( line -> {
                    String[] parts = line;
                    double[] elements = new double[line.length];

                    for (int i = 1; i < parts.length; i++) {
                        elements[i]=Double.parseDouble(parts[i]);
                    }

                    return new LabeledPoint(Double.parseDouble(parts[0]),
                            Vectors.dense(elements));
                });



        parsedData.foreach(f -> System.out.println(Arrays.asList(f)));
//        System.out.println(parsedData.first().toString());











//        JavaRDD<String> lines = jsc.textFile("data/mllib/convert_csv.txt");
//        JavaRDD<Iterable<String>> words_iterable = lines.map( s -> {
//            String[] words = s.split(",");
//            Iterable<String> output = Arrays.asList(words);
//            return output;
//        });
//        Word2Vec word2vec = new Word2Vec();
//        Word2VecModel word2vecmodel =  word2vec.fit(words_iterable);
//
//        System.out.println( word2vecmodel.transform("cipa"));


    }
    public static boolean isNumerical(String s) {
        boolean isValidInteger = false;
        try {
            //Integer.parseInt(s);
            Double.parseDouble(s);
            // s is a valid integer
            isValidInteger = true;
        } catch (NumberFormatException ex) {
            // s is not an integer
        }

        return isValidInteger;
    }
}
