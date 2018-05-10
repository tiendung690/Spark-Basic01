package myimplementation;

import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
import scala.StringContext;
import scala.Tuple2;

/**
 * Created by as on 10.05.2018.
 */
public class FastDot {
    public double dot(final Vector x, final Vector y) {
        if(x.size() != y.size()){
            System.out.println("x.size != y.size");
            return Double.parseDouble(null);
        }

        Tuple2 var3 = new Tuple2(x, y);
        double var8;
        if(var3 != null) {
            Vector dx = (Vector)var3._1();
            Vector dy = (Vector)var3._2();
            if(dx instanceof DenseVector) {
                DenseVector var6 = (DenseVector)dx;
                if(dy instanceof DenseVector) {
                    DenseVector var7 = (DenseVector)dy;
                    var8 = this.dot(var6, var7);
                    return var8;
                }
            }
        }

        if(var3 != null) {
            Vector sx = (Vector)var3._1();
            Vector dy = (Vector)var3._2();
            if(sx instanceof SparseVector) {
                SparseVector var12 = (SparseVector)sx;
                if(dy instanceof DenseVector) {
                    DenseVector var13 = (DenseVector)dy;
                    var8 = this.dot(var12, var13);
                    return var8;
                }
            }
        }

        if(var3 != null) {
            Vector dx = (Vector)var3._1();
            Vector sy = (Vector)var3._2();
            if(dx instanceof DenseVector) {
                DenseVector var16 = (DenseVector)dx;
                if(sy instanceof SparseVector) {
                    SparseVector var17 = (SparseVector)sy;
                    var8 = this.dot(var17, var16);
                    return var8;
                }
            }
        }

        if(var3 != null) {
            Vector sx = (Vector)var3._1();
            Vector sy = (Vector)var3._2();
            if(sx instanceof SparseVector) {
                SparseVector var20 = (SparseVector)sx;
                if(sy instanceof SparseVector) {
                    SparseVector var21 = (SparseVector)sy;
                    var8 = this.dot(var20, var21);
                    return var8;
                }
            }
        }

        throw new IllegalArgumentException();
    }

    private double dot(DenseVector x, DenseVector y) {
//        int n = x.size();
//        return this.f2jBLAS().ddot(n, x.values(), 1, y.values(), 1);
        double pom = 0;
        for (int i = 0; i < x.size(); i++) {
            pom += x.apply(i) * y.apply(i);
        }
        return pom;
    }

    private double dot(SparseVector x, DenseVector y) {
        double[] xValues = x.values();
        int[] xIndices = x.indices();
        double[] yValues = y.values();
        int nnz = xIndices.length;
        double sum = 0.0D;

        for(int k = 0; k < nnz; ++k) {
            sum += xValues[k] * yValues[xIndices[k]];
        }

        return sum;
    }

    private double dot(SparseVector x, SparseVector y) {
        double[] xValues = x.values();
        int[] xIndices = x.indices();
        double[] yValues = y.values();
        int[] yIndices = y.indices();
        int nnzx = xIndices.length;
        int nnzy = yIndices.length;
        int kx = 0;
        int ky = 0;

        double sum;
        for(sum = 0.0D; kx < nnzx && ky < nnzy; ++kx) {
            int ix;
            for(ix = xIndices[kx]; ky < nnzy && yIndices[ky] < ix; ++ky) {
                ;
            }

            if(ky < nnzy && yIndices[ky] == ix) {
                sum += xValues[kx] * yValues[ky];
                ++ky;
            }
        }

        return sum;
    }
}
