package kmeans_implementation.myimplOldFiles;

import org.apache.spark.mllib.linalg.Vectors;
import scala.math.package$;

/**
 * Created by as on 05.08.2018.
 */
public class OldDistances {
    static double dot(org.apache.spark.mllib.linalg.Vector v1, org.apache.spark.mllib.linalg.Vector v2) {

        double pom = 0;
        for (int i = 0; i < v1.size(); i++) {
            pom += v1.apply(i) * v2.apply(i);
        }
        return pom;
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
}
