package kmeans_implementation.old_files;

import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.util.MLUtils$;

import java.util.Arrays;

/**
 * Created by as on 24.04.2018.
 */
public class VectorSpeedTest {
    public static void main(String[] args) {

        Vector point1 = new DenseVector(new double[]{1, 0, 0, 0, 0});
        Vector point2 = new DenseVector(new double[]{1, 2, 3, 4, 5});
        //System.out.println("fast: " + fastDist(point1, point2));
        //System.out.println("norm: " + distanceEuclidean2(point1, point2));


        //BLAS.axpy(1.0, point2, point1);
        BLAS.scal(1.0 / 2, point2);

        System.out.println(Arrays.toString(point1.toArray()));
        System.out.println(Arrays.toString(point2.toArray()));

    }

    public static double distanceEuclidean2(Vector t1, Vector t2) {
        double sum = 0;
        for (int i = 0; i < t1.size(); i++) {
            sum += Math.pow((t1.apply(i) - t2.apply(i)), 2.0);
        }
        return Math.sqrt(sum);
    }

    public static double fastDist(Vector point1, Vector point2) {
        double d = MLUtils$.MODULE$.fastSquaredDistance(Vectors.fromML(point1),
                Vectors.norm(Vectors.fromML(point1), 2.0),
                Vectors.fromML(point2), Vectors.norm(Vectors.fromML(point2), 2.0),
                0);
        return d;
    }



}
