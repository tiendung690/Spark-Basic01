package kmeansimplementation;

import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.Vector;

/**
 * Created by as on 26.04.2018.
 */
public class Distances {


    public static double distanceCosine(Vector v1, Vector v2) {
        return 1 - BLAS.dot(v1, v2) / org.apache.spark.ml.linalg.Vectors.norm(v1, 2.0) / org.apache.spark.ml.linalg.Vectors.norm(v2, 2.0);
    }

    public static double distanceCosine(double[] t1, double[] t2) {
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < t1.length; i++) {
            dotProduct += t1[i] * t2[i];
            normA += Math.pow(t1[i], 2);
            normB += Math.pow(t2[i], 2);
        }
        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    public static double distanceSquared(double[] t1, double[] t2) {
        double distance = 0.0;
        int size = t1.length;
        for (int i = 0; i < size; i++) {
            double diff = t1[i] - t2[i];
            distance += diff * diff;
        }
        return distance;
    }

    public static double distanceEuclidean(double[] t1, double[] t2) {
        double sum = 0;
        for (int i = 0; i < t1.length; i++) {
            sum += Math.pow((t1[i] - t2[i]), 2.0);
        }
        return Math.sqrt(sum);
    }

    public static double distanceManhattan(double[] t1, double[] t2) {
        double sum = 0;
        for (int i = 0; i < t1.length; i++) {
            sum += Math.abs((t1[i] - t2[i]));
        }
        return sum;
    }

    public static double distanceMinkowski(double[] t1, double[] t2) {
        double lambda = 3.0;
        double sum = 0;
        for (int i = 0; i < t1.length; i++) {
            sum += Math.pow(Math.abs(t1[i] - t2[i]), lambda);
        }
        return Math.pow(sum, 1.0 / lambda);
    }

    public static double distanceChebyshev(double[] t1, double[] t2) {
        double max = Math.abs(t1[0] - t2[0]);
        for (int i = 1; i < t1.length; i++) {
            double abs = Math.abs(t1[i] - t2[i]);
            if (abs > max) max = abs;
        }
        return max;
    }
}
