package myimplOldFiles;

import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.SparseVector;
import scala.Option;
import scala.Tuple2;
import scala.collection.mutable.StringBuilder;
import scala.runtime.BoxedUnit;

/**
 * Created by as on 10.05.2018.
 */
public class FastSqdist {
    public static void main(String[] args) {

        DenseVector d1 = new DenseVector(new double[]{1, 2, 3, 4, 5});
        DenseVector d2 = new DenseVector(new double[]{1, 2, 3, 4, 7});
        FastSqdist fs = new FastSqdist();
        System.out.println(fs.sqdist(d1, d2));

    }

    public double sqdist(final org.apache.spark.mllib.linalg.Vector v11, final org.apache.spark.mllib.linalg.Vector v22) {

        if (v11.size() != v22.size()) {
            System.out.println("v1 size != v2 size");
            return Double.parseDouble(null);
        }

        double squaredDistance = 0.0D;
        Tuple2 var5 = new Tuple2(v11, v22);
        BoxedUnit var10;
        if (var5 != null) {
            org.apache.spark.mllib.linalg.Vector v1 = (org.apache.spark.mllib.linalg.Vector) var5._1();
            org.apache.spark.mllib.linalg.Vector v2 = (org.apache.spark.mllib.linalg.Vector) var5._2();
            if (v1 instanceof SparseVector) {
                SparseVector var8 = (SparseVector) v1;
                if (v2 instanceof SparseVector) {
                    SparseVector var9 = (SparseVector) v2;
                    double[] v1Values = var8.values();
                    int[] v1Indices = var8.indices();
                    double[] v2Values = var9.values();
                    int[] v2Indices = var9.indices();
                    int nnzv1 = v1Indices.length;
                    int nnzv2 = v2Indices.length;
                    int kv1 = 0;

                    double score = 0;
                    for (int kv2 = 0; kv1 < nnzv1 || kv2 < nnzv2; squaredDistance += score * score) {
                        score = 0.0D;
                        if (kv2 < nnzv2 && (kv1 >= nnzv1 || v1Indices[kv1] >= v2Indices[kv2])) {
                            if (kv1 < nnzv1 && (kv2 >= nnzv2 || v2Indices[kv2] >= v1Indices[kv1])) {
                                score = v1Values[kv1] - v2Values[kv2];
                                ++kv1;
                                ++kv2;
                            } else {
                                score = v2Values[kv2];
                                ++kv2;
                            }
                        } else {
                            score = v1Values[kv1];
                            ++kv1;
                        }
                    }

                    var10 = BoxedUnit.UNIT;
                    return squaredDistance;
                }
            }
        }

        if (var5 != null) {
            org.apache.spark.mllib.linalg.Vector v1 = (org.apache.spark.mllib.linalg.Vector) var5._1();
            org.apache.spark.mllib.linalg.Vector v2 = (org.apache.spark.mllib.linalg.Vector) var5._2();
            if (v1 instanceof SparseVector) {
                SparseVector var23 = (SparseVector) v1;
                if (v2 instanceof DenseVector) {
                    DenseVector var24 = (DenseVector) v2;
                    squaredDistance = this.sqdist(var23, var24);
                    var10 = BoxedUnit.UNIT;
                    return squaredDistance;
                }
            }
        }

        if (var5 != null) {
            org.apache.spark.mllib.linalg.Vector v1 = (org.apache.spark.mllib.linalg.Vector) var5._1();
            org.apache.spark.mllib.linalg.Vector v2 = (org.apache.spark.mllib.linalg.Vector) var5._2();
            if (v1 instanceof DenseVector) {
                DenseVector var27 = (DenseVector) v1;
                if (v2 instanceof SparseVector) {
                    SparseVector var28 = (SparseVector) v2;
                    squaredDistance = this.sqdist(var28, var27);
                    var10 = BoxedUnit.UNIT;
                    return squaredDistance;
                }
            }
        }

        if (var5 != null) {
            org.apache.spark.mllib.linalg.Vector var29 = (org.apache.spark.mllib.linalg.Vector) var5._1();
            org.apache.spark.mllib.linalg.Vector var30 = (org.apache.spark.mllib.linalg.Vector) var5._2();
            if (var29 instanceof DenseVector) {
                DenseVector var31 = (DenseVector) var29;
                Option var32 = org.apache.spark.mllib.linalg.DenseVector.unapply(var31);
                if (!var32.isEmpty()) {
                    double[] vv1 = (double[]) var32.get();
                    if (var30 instanceof DenseVector) {
                        DenseVector var34 = (DenseVector) var30;
                        Option var35 = org.apache.spark.mllib.linalg.DenseVector.unapply(var34);
                        if (!var35.isEmpty()) {
                            double[] vv2 = (double[]) var35.get();
                            int kv = 0;
                            for (int sz = vv1.length; kv < sz; ++kv) {
                                double score = vv1[kv] - vv2[kv];
                                squaredDistance += score * score;
                            }
                            var10 = BoxedUnit.UNIT;
                            return squaredDistance;
                        }
                    }
                }
            }
        }

        throw new IllegalArgumentException((new StringBuilder()).append("Do not support vector type ").append(v11.getClass()).append(" and ").append(v22.getClass()).toString());
    }

    public double sqdist(SparseVector v1, DenseVector v2) {
        int kv1 = 0;
        int kv2 = 0;
        int[] indices = v1.indices();
        double squaredDistance = 0.0D;
        int nnzv1 = indices.length;
        int nnzv2 = v2.size();

        for(int iv1 = nnzv1 > 0?indices[kv1]:-1; kv2 < nnzv2; ++kv2) {
            double score = 0.0D;
            if(kv2 != iv1) {
                score = v2.apply(kv2);
            } else {
                score = v1.values()[kv1] - v2.apply(kv2);
                if(kv1 < nnzv1 - 1) {
                    ++kv1;
                    iv1 = indices[kv1];
                }
            }

            squaredDistance += score * score;
        }

        return squaredDistance;
    }
}
