package myimplementation.myimplOldFiles;

import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.clustering.VectorWithNorm;
import org.apache.spark.mllib.linalg.Vectors;
import scala.runtime.BoxedUnit;

/**
 * Created by as on 10.05.2018.
 */
public class FastAxpy {

    public static void main(String[] args) {
        Vector d1 = new DenseVector(new double[]{1, 2, 3, 4, 5});
        Vector d2 = new DenseVector(new double[]{2, 2, 2, 2, 2});

        System.out.println(new VectorWithNorm(Vectors.fromML(d1)).norm());
//        FastAxpy fa = new FastAxpy();
//        fa.axpy(2.0, d1, d2);
//        System.out.println(Arrays.toString(d1.toArray()));
//        System.out.println(Arrays.toString(d2.toArray()));
    }

    public Vector axpy(double a, Vector x, Vector y) {

        if (x.size() != y.size()) {
            System.out.println("v1 size != v2 size");
            // return Double.parseDouble(null);
        }

        if (y instanceof DenseVector) {
            DenseVector var6 = (DenseVector) y;
            BoxedUnit var10;
            if (x instanceof SparseVector) {
                SparseVector var9 = (SparseVector) x;
                //this.axpy(a, var9, var6);
                //var10 = BoxedUnit.UNIT;
                return this.axpy(a, var9, var6);
            } else {
                if (!(x instanceof DenseVector)) {
                    throw new UnsupportedOperationException("axpy doesn't support x type ");
                }

                DenseVector var11 = (DenseVector) x;
                //this.axpy(a, var11, var6);
                //var10 = BoxedUnit.UNIT;
                return this.axpy(a, var11, var6);
            }

            //BoxedUnit var7 = BoxedUnit.UNIT;
        } else {
            throw new IllegalArgumentException("axpy only supports adding to a dense vector but got type ");
        }
    }

    private Vector axpy(double a, DenseVector x, DenseVector y) {

//        DenseVector d1 = x.copy();
//        DenseVector d2 = y.copy();
//
//        int n = x.size();
//        F2jBLAS.getInstance().daxpy(n, a, d1.values(), 1, d2.values(), 1);

        double[] xValues = x.values();
        double[] yValues = y.values();
        int nnz = x.size();
        if (a == 1.0D) {
            for (int k = 0; k < nnz; ++k) {
                yValues[k] += xValues[k];
            }
        } else {
            for (int k = 0; k < nnz; ++k) {
                yValues[k] += a * xValues[k];
            }
        }

        //Vector vv = new DenseVector(d2.values());
        Vector vv = new DenseVector(yValues);

        return vv;
    }

    private Vector axpy(double a, SparseVector x, DenseVector y) {
        double[] xValues = x.values();
        int[] xIndices = x.indices();
        double[] yValues = y.values();
        int nnz = xIndices.length;
        if (a == 1.0D) {
            for (int k = 0; k < nnz; ++k) {
                int var10 = xIndices[k];
                yValues[var10] += xValues[k];
            }
        } else {
            for (int k = 0; k < nnz; ++k) {
                int var12 = xIndices[k];
                yValues[var12] += a * xValues[k];
            }
        }

        Vector vv = new DenseVector(yValues);

        return vv;

    }
}
