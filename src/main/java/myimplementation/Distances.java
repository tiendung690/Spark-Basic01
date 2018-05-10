package myimplementation;

import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Option;
import scala.Serializable;
import scala.StringContext;
import scala.Tuple2;
import scala.collection.mutable.StringBuilder;
import scala.math.package$;
import scala.runtime.BoxedUnit;
import scala.runtime.BoxesRunTime;

/**
 * Created by as on 26.04.2018.
 */
public class Distances {

    public static double fastSquaredDistance(org.apache.spark.mllib.linalg.Vector v1, double norm1, org.apache.spark.mllib.linalg.Vector v2, double norm2) {
        double epsilon = 1e-4;
        double precision = 1e-6;
        double sumSquaredNorm = norm1 * norm1 + norm2 * norm2;
        double normDiff = norm1 - norm2;
        double sqDist = 0.0;
        double precisionBound1 = 2.0D * epsilon * sumSquaredNorm / (normDiff * normDiff + epsilon);
        if (precisionBound1 < precision) {
            // System.out.println("precisionBound1 < precision");
            sqDist = sumSquaredNorm - 2.0D * org.apache.spark.mllib.linalg.BLAS$.MODULE$.dot(v1, v2);
        } else if (!(v1 instanceof org.apache.spark.mllib.linalg.SparseVector) && !(v2 instanceof org.apache.spark.mllib.linalg.SparseVector)) {
            //  System.out.println("INSTANCEOF NOT SPARSE");
            sqDist = Vectors.sqdist(v1, v2);
        } else {
            //System.out.println("---------------");
            double dotValue = org.apache.spark.mllib.linalg.BLAS$.MODULE$.dot(v1, v2);
            sqDist = package$.MODULE$.max(sumSquaredNorm - 2.0D * dotValue, 0.0D);
            double precisionBound2 = epsilon * (sumSquaredNorm + 2.0D * package$.MODULE$.abs(dotValue)) / (sqDist + epsilon);
            if (precisionBound2 > precision) {
                //   System.out.println("XXXXXXX");
                sqDist = Vectors.sqdist(v1, v2);
            } else {
                // System.out.println("---");
            }
        }
        return sqDist;
    }

    public static double fastSquaredDistance_V2(org.apache.spark.mllib.linalg.Vector v1, double norm1, org.apache.spark.mllib.linalg.Vector v2, double norm2) {
        double epsilon = 1e-1;
        double precision = 1e-6;
        double sumSquaredNorm = norm1 * norm1 + norm2 * norm2;
        double normDiff = norm1 - norm2;
        double sqDist = 0.0;
        double precisionBound1 = 2.0D * epsilon * sumSquaredNorm / (normDiff * normDiff + epsilon);
        if (precisionBound1 < precision) {
            // System.out.println("precisionBound1 < precision");
            sqDist = new FastDot().dot(v1, v2);
        } else if (!(v1 instanceof org.apache.spark.mllib.linalg.SparseVector) && !(v2 instanceof org.apache.spark.mllib.linalg.SparseVector)) {
            //  System.out.println("INSTANCEOF NOT SPARSE");
            sqDist = new FastSqdist().sqdist(v1,v2);
        } else {
            //System.out.println("---------------");
            double dotValue = new FastDot().dot(v1, v2);
            sqDist = Math.max(sumSquaredNorm - 2.0D * dotValue, 0.0D);
            double precisionBound2 = epsilon * (sumSquaredNorm + 2.0D * Math.abs(dotValue)) / (sqDist + epsilon);
            if (precisionBound2 > precision) {
                //   System.out.println("XXXXXXX");
                sqDist =  new FastSqdist().sqdist(v1,v2);
            } else {
                // System.out.println("---");
            }
        }
        return sqDist;
    }

    static double dot(org.apache.spark.mllib.linalg.Vector v1, org.apache.spark.mllib.linalg.Vector v2) {

        double pom = 0;
        for (int i = 0; i < v1.size(); i++) {
            pom += v1.apply(i) * v2.apply(i);
        }
        return pom;
    }


    static double cosineDistance(Vector v1, Vector v2) {
        return 1 - BLAS.dot(v1, v2) / org.apache.spark.ml.linalg.Vectors.norm(v1, 2.0) / org.apache.spark.ml.linalg.Vectors.norm(v2, 2.0);
    }

    static double squaredDistance(Vector a, Vector b) {
        double distance = 0.0;
        int size = a.size();
        for (int i = 0; i < size; i++) {
            double diff = a.apply(i) - b.apply(i);
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

    public static double distanceEuclideanSquared(Vector t1, Vector t2) {
        //System.out.println("########: "+t1.size()+",  "+t2.size());
        //return org.apache.spark.ml.linalg.Vectors.sqdist(t1,t2);
        double sum = 0;
        for (int i = 0; i < t1.size(); i++) {
            sum += Math.pow((t1.apply(i) - t2.apply(i)), 2.0);
        }
        return sum;
    }

    public static double distanceEuclidean2(Vector t1, Vector t2) {
        //System.out.println("########: "+t1.size()+",  "+t2.size());
        //return org.apache.spark.ml.linalg.Vectors.sqdist(t1,t2);
        double sum = 0;
        for (int i = 0; i < t1.size(); i++) {
            sum += Math.pow((t1.apply(i) - t2.apply(i)), 2.0);
        }
        return Math.sqrt(sum);
    }

    public static double distanceEuclidean2(org.apache.spark.mllib.linalg.Vector t1, org.apache.spark.mllib.linalg.Vector t2) {
        //System.out.println("########: "+t1.size()+",  "+t2.size());
        //return org.apache.spark.ml.linalg.Vectors.sqdist(t1,t2);
        double sum = 0;
        for (int i = 0; i < t1.size(); i++) {
            sum += Math.pow((t1.apply(i) - t2.apply(i)), 2.0);
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
