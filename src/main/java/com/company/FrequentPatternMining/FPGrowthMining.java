package com.company.FrequentPatternMining;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.util.Arrays;
import java.util.List;

/**
 * Created by as on 29.11.2017.
 */
public class FPGrowthMining {

    // FREQUENT PATTERN MINING
    public static void fpGrowth(JavaSparkContext jsc, String path, double minSupport, double minConfidence) {

        System.out.println("\nFP-Growth----------------------------");

        // Wczytanie danych

        JavaRDD<String> data = jsc.textFile(path);

        // Przekazanie danych do kolekcji przy pomocy separatora

        JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split(",")));
        //double minSupport = 0.02;
        System.out.println("Minimal Support: "+minSupport);

        // Ustawienie parametrów algorytmu

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSupport)
                .setNumPartitions(20);

        // Stworzenie modelu

        FPGrowthModel<String> model = fpg.run(transactions);

        // Wypisanie danych przy zadanym wsparciu

        for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
            System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
        }


        System.out.println("Assoc rules-------------------------------");
        //double minConfidence = 0.3;
        System.out.println("Minimal Confidence: "+minConfidence);

        // Wypisanie reguł asocjacyjnych przy zadanej pewności

        for (AssociationRules.Rule<String> rule
                : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
            System.out.println(
                    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }

    }
}
