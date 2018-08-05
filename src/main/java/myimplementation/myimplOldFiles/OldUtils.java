package myimplementation.myimplOldFiles;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by as on 03.08.2018.
 */
public class OldUtils {
    public static Vector sumArrayByColumnSparse(SparseVector t1, SparseVector t2) {
        double[] tab = new double[t1.size()];
        for (int i = 0; i < t1.size(); i++) {
            tab[i] = t1.apply(i) + t2.apply(i);
        }
        return new DenseVector(tab);
    }

    public static Vector sumArrayByColumn2(Vector t1, Vector t2) {
        if (t1 instanceof DenseVector && t2 instanceof DenseVector) {

            double[] tab = new double[t1.size()];
            for (int i = 0; i < t1.size(); i++) {
                tab[i] = t1.apply(i) + t2.apply(i);
            }
            return new DenseVector(tab);

        } else if (t1 instanceof SparseVector && t2 instanceof SparseVector) {
            SparseVector v1 = (SparseVector) t1;
            SparseVector v2 = (SparseVector) t2;

            double[] tab = new double[v1.size()];
            for (int i = 0; i < v1.size(); i++) {
                tab[i] = t1.apply(i) + t2.apply(i);
            }
            return new DenseVector(tab);

        } else if (t1 instanceof SparseVector && t2 instanceof DenseVector) {

        } else if (t1 instanceof DenseVector && t2 instanceof SparseVector) {

        } else {

        }
        return null;
    }

    public static int findLowerValIndexNew(Vector tab) {

        int index = 0;
        double min = tab.apply(index);
        for (int i = 1; i < tab.size(); i++) {
            if (tab.apply(i) < min) {
                min = tab.apply(i);
                index = i;
            }
        }
        return index;
    }

    public static double[] sumArrayByColumnOld(double[] t1, double[] t2) {
        double[] tab = new double[t1.length];
        for (int i = 0; i < t1.length; i++) {
            tab[i] = t1[i] + t2[i];
        }
        return tab;
    }

    public static JavaPairRDD<Integer, Vector> convertToRDDModel2(Dataset<Row> ds) {
        // Convert dataset to JavaRDD of DataModel
        JavaPairRDD<Integer, Vector> x3 = ds.toJavaRDD().mapToPair(row -> {
            return new Tuple2<>(0, (Vector) row.get(0));
        });
        return x3;//.repartition(4);
        //return x3.repartition(SparkContext.getOrCreate().defaultParallelism());
    }

    public static JavaRDD<Vector> convertToRDDModel4(Dataset<Row> ds) {
        // Convert dataset to JavaRDD of DataModel
        JavaRDD<Vector> x3 = ds.toJavaRDD().map(row -> (Vector) row.get(0));
        return x3;//.repartition(4);
        //return x3.repartition(SparkContext.getOrCreate().defaultParallelism());
    }

    public static JavaPairRDD<Integer, org.apache.spark.mllib.linalg.Vector> convertToRDDModel3(Dataset<Row> ds) {
        JavaPairRDD<Integer, org.apache.spark.mllib.linalg.Vector> x3 = ds.toJavaRDD().mapToPair(row -> {
            return new Tuple2<Integer, Vector>(0, (Vector) row.get(0));
        }).mapToPair(v1 -> {
            return new Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>(v1._1(), Vectors.fromML(v1._2()));
        });
        return x3;
    }

    public static void printCenters(ArrayList<Vector> v) {
        System.out.println("Centers:");
        for (int i = 0; i < v.size(); i++) {
            System.out.println(Arrays.toString(v.get(i).toArray()));
        }
    }

    public static void printCenters2(ArrayList<org.apache.spark.mllib.linalg.Vector> v) {
        System.out.println("Centers:");
        for (int i = 0; i < v.size(); i++) {
            System.out.println(Arrays.toString(v.get(i).toArray()));
        }
    }
}
