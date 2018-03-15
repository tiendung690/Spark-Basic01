package com.company.Clustering;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * Created by as on 29.11.2017.
 */
public class KMeansClustering {

    // CLUSTERING K-MEANS
    public static void clusteringKMeans(JavaSparkContext jsc, String path, int numClusters) {

        //        1.0 1.0
        //        1.5 2.0
        //        3.0 4.0
        //        5.0 7.0
        //        3.5 5.0
        //        4.5 5.0
        //        3.5 4.5

        System.out.println("\nK-Means----------------------------");

        // Load and parse data
        //String path = "data/mllib/kmeans_data_NEW.txt";
        //String path = "data/mllib/crime_data.csv";
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(" ");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Double.parseDouble(sarray[i]);
        //System.out.println("val: " + values[i]);
            }
            return Vectors.dense(values);
        });
        parsedData.cache();

        // Cluster the data into * classes using KMeans
        //int numClusters = 3;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
        System.out.println("Clusters: " + numClusters);
        System.out.println("Cluster centers:");
        int i = 0;
        for (Vector center : clusters.clusterCenters()) {
            System.out.println("cluster: " + i + " " + center + " predict: " + clusters.predict(center));
            i++;
        }

        // wypisanie klastra dla kazdego obiektu
        parsedData.foreach(rdd -> {
            System.out.println("For object: " + rdd + " predicted cluster: " + clusters.predict(rdd) + " ");
        });

        double cost = clusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost);
        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        //clusters.save(jsc.sc(), "target/org/apache/spark/JavaKMeansExample/KMeansModel");

    }
}
