package myimplementation;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by as on 26.04.2018.
 */
public class Util {

    public static JavaRDD<Kmns.DataModel> convertToRDDModel(Dataset<Row> ds) {
        // Convert dataset to JavaRDD of DataModel
        JavaRDD<Kmns.DataModel> x3 = ds.toJavaRDD().map(row -> {
            Kmns.DataModel dataModel = new Kmns.DataModel();
            dataModel.setInputData((Vector) row.get(0));
            return dataModel;
        });
        return x3;//.repartition(4);
        //return x3.repartition(SparkContext.getOrCreate().defaultParallelism());
    }

    public static JavaPairRDD<Integer, Vector> convertToRDDModel2(Dataset<Row> ds) {
        // Convert dataset to JavaRDD of DataModel
        JavaPairRDD<Integer, Vector> x3 = ds.toJavaRDD().mapToPair(row -> {
            return new Tuple2<>(0, (Vector) row.get(0));
        });
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

    public static void saveAsCSV(Dataset<Row> dm) {

        // zapis do pliku w formacie csv (po przecinku), bez headera
        JavaRDD<String> rr = dm.select("features", "prediction").toJavaRDD().map(value -> {
            Vector vector = (Vector) value.get(0);
            Integer s = (Integer) value.get(1);
            Vector vector2 = new DenseVector(ArrayUtils.addAll(vector.toArray(), new double[]{s.doubleValue()}));
            return Arrays.toString(vector2.toArray())
                    .replace("[", "")
                    .replace("]", "")
                    .replaceAll(" ", "");
        });
        //System.out.println(rr.first());
        // brak mozliwosci nadpisania, lepiej zrobic dataframe i wtedy zapisywac z mode overwrite
        rr.coalesce(1).saveAsTextFile("data/ok");
    }

    public static int findLowerValIndex(double[] tab) {

        int index = 0;
        double min = tab[index];
        for (int i = 1; i < tab.length; i++) {
            if (tab[i] < min) {
                min = tab[i];
                index = i;
            }
        }
        return index;
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

    public static Vector sumArrayByColumn(Vector t1, Vector t2) {
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
            SparseVector v1 = (SparseVector)t1;
            SparseVector v2 = (SparseVector)t2;

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

    public static Vector sumArrayByColumnSparse(SparseVector t1, SparseVector t2) {
        double[] tab = new double[t1.size()];
        for (int i = 0; i < t1.size(); i++) {
            tab[i] = t1.apply(i) + t2.apply(i);
        }
        return new DenseVector(tab);
    }

    public static Vector divideArray(Vector t1, Long l) {
        double[] tab = new double[t1.size()];
        for (int i = 0; i < t1.size(); i++) {
            tab[i] = t1.apply(i) / l;
        }
        return new DenseVector(tab);
    }

    // Transform JavaRDD<DataModel> -> Dataset<Row>  (VectorUDF)
    public static Dataset<Row> createDataSetUDF(JavaRDD<Kmns.DataModel> x, SparkSession spark, String featuresCol, String predictionCol) {
        JavaRDD<Row> ss = x.map(v1 -> RowFactory.create(v1.getInputData(), v1.getCluster()));
        // new StructType
        StructType schema = new StructType(new StructField[]{
                new StructField(featuresCol, new VectorUDT(), false, Metadata.empty()),
                new StructField(predictionCol, DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dm = spark.createDataFrame(ss, schema);
        return dm;
    }

    // Transform JavaPairRDD<DataModel> -> Dataset<Row>  (VectorUDF)
    public static Dataset<Row> createDataSet2(JavaPairRDD<Integer, Vector> x, SparkSession spark, String featuresCol, String predictionCol) {
        JavaRDD<Row> ss = x.map(v1 -> RowFactory.create(v1._2(), v1._1()));
        // new StructType
        StructType schema = new StructType(new StructField[]{
                new StructField(featuresCol, new VectorUDT(), false, Metadata.empty()),
                new StructField(predictionCol, DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dm = spark.createDataFrame(ss, schema);
        return dm;
    }
}
