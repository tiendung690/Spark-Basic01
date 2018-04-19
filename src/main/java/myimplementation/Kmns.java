package myimplementation;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.ClusteringSummary;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansSummary;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Created by as on 09.04.2018.
 */
public class Kmns {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("KMeans_Implementation_Euclidean_normalArray")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                //.set("spark.driver.memory", "4g")
                //.set("spark.executor.memory", "4g")
                .setMaster("local[*]");

//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Experiment_Implementation_Kmeans")
//                .setMaster("spark://10.2.28.17:7077")
//                .setJars(new String[]{"out/artifacts/SparkProject_jar/SparkProject.jar"})
//                //.set("spark.executor.memory", "15g")
//                //.set("spark.executor.cores", "10")
//                //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                //.set("spark.submit.deployMode", "cluster")
//                //.set("spark.default.parallelism", "24")
//                .set("spark.driver.host", "10.2.28.31");

//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Experiment_Implementation_Kmeans")
//                .setMaster("spark://192.168.100.4:7077")
//                .setJars(new String[] { "out/artifacts/SparkProject_jar/SparkProject.jar" })
//                //.set("spark.executor.memory", "15g")
//                //.set("spark.default.parallelism", "12")
//                .set("spark.driver.host", "192.168.100.2");

        SparkContext sc = new SparkContext(conf);
        SparkSession spark = new SparkSession(sc);

        System.out.println("**********" + sc.defaultParallelism() + "  ," + sc.defaultMinPartitions());

        //String path = "hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt.gz";
        //String path = "hdfs://192.168.100.4:9000/spark/kdd_10_proc.txt.gz";
        //String path = "data/mllib/kdd_10_proc.txt.gz";
        String path = "data/mllib/kdd_10_proc.txt";
        //String path = "data/mllib/kddFIX.txt";
        //String path = "data/mllib/kddcup_train.txt";
        //String path = "data/mllib/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup.txt";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kmean.txt";
        //String path = "data/mllib/kmean.txt";
        //String path = "data/mllib/iris2.csv";
        //String path = "data/mllib/creditcard.csv";
        //String path = "hdfs:/192.168.100.4/data/mllib/kmean.txt";

        // load mem data
        MemDataSet memDataSet = new MemDataSet(spark);
        memDataSet.loadDataSet(path);

        DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> ds1 = dpc.prepareDataset(memDataSet.getDs(), false, true);
        Dataset<Row> ds = ds1.select("features");
//        ds.show();
//        ds.printSchema();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Convert dataset to JavaRDD of Vectors
        JavaRDD<Vector> x3 = convertToRDD(ds);
        //System.out.println("NUM PARTITIONS: " + x33.getNumPartitions());
        //JavaRDD<Vector> x3 = x33.repartition(4);
        x3.cache();
        //System.out.println("NUM PARTITIONS: " + x3.getNumPartitions());
        // Take starting points
        //List<Vector> startingCenters = x3.takeSample(false, 2);
        int k = 26;
        ArrayList<Vector> clusterCenters = new ArrayList<>(x3.takeSample(false, k, 20L));
        //ArrayList<Vector> clusterCenters = new ArrayList<>();
        //clusterCenters.add(new DenseVector(new double[]{4.8,3.0,1.4,0.3,0.0,1.0}));
        //clusterCenters.add(new DenseVector(new double[]{6.9,3.1,4.9,1.5,1.0,0.0}));
        System.out.println("Starting centers:" + Arrays.toString(clusterCenters.toArray()));

        ArrayList<Vector> clusterCenters2 = computeCenters(x3, clusterCenters);
        // Compute distances, Predict Cluster
        JavaRDD<DataModel> x5 = computeDistancesAndPredictCluster(x3, clusterCenters2);

        Dataset<Row> dm = createDataSetUDF(x5, spark, "features", "prediction");
        dm.show(false);
        dm.printSchema();

        printCenters(clusterCenters);

        ClusteringEvaluator clusteringEvaluator = new ClusteringEvaluator();
        clusteringEvaluator.setFeaturesCol("features");
        clusteringEvaluator.setPredictionCol("prediction");
        System.out.println("EVAL: " + clusteringEvaluator.evaluate(dm));

        ClusteringSummary clusteringSummary = new ClusteringSummary(dm, "prediction", "features", k);
        System.out.println(Arrays.toString(clusteringSummary.clusterSizes()));

        //saveAsCSV(dm);
        //new Scanner(System.in).nextLine();
        spark.close();
    }

    public static ArrayList<Vector> computeCenters(JavaRDD<Vector> x3, ArrayList<Vector> cc) {

        ArrayList<Vector> clusterCenters = new ArrayList<>(cc);

        int max_iter = 20;
        boolean bol = true;
        int ii = 0;
        do {
            // Compute distances, Predict Cluste
            JavaRDD<DataModel> x5 = computeDistancesAndPredictCluster(x3, clusterCenters);

            ArrayList<Vector> clusterCenters2 = new ArrayList<>(clusterCenters);
            // Update center
            for (int i = 0; i < clusterCenters.size(); i++) {
                clusterCenters2.set(i, mean(x5, i, clusterCenters2.get(i)));
                System.out.println(i + " ," + Arrays.toString(clusterCenters2.get(i).toArray()));
            }
            if (clusterCenters2.equals(clusterCenters) || ii == max_iter) {
                System.out.println("END" + Arrays.toString(clusterCenters2.toArray()));
                bol = false;
            } else {
                clusterCenters = clusterCenters2;
                ii++;
                System.out.println("ITERATION: " + ii + " " + Arrays.toString(clusterCenters2.toArray()));
            }
        } while (bol);

        return clusterCenters;
    }

    public static JavaRDD<DataModel> computeDistancesAndPredictCluster(JavaRDD<Vector> x3, ArrayList<Vector> clusterCenters) {
        // Compute distances
        JavaRDD<DataModel> x4 = computeDistances(x3, clusterCenters);
        // Predict Cluster
        JavaRDD<DataModel> x5 = predictCluster(x4);
        return x5;
    }

    public static JavaRDD<Vector> convertToRDD(Dataset<Row> ds) {
        // Convert dataset to JavaRDD of Vectors
        JavaRDD<Vector> x3 = ds.toJavaRDD().map(row -> (Vector) row.get(0));
        return x3;
    }


    public static void printCenters(ArrayList<Vector> v) {
        System.out.println("Centers:");
        for (int i = 0; i < v.size(); i++) {
            System.out.println(Arrays.toString(v.get(i).toArray()));
        }
    }

    public static void saveAsCSV(Dataset<Row> dm) {

        // zapis do pliku w formacie csv (po przecinku), bez headera
        JavaRDD<String> rr = dm.toJavaRDD().map(value -> {
            Vector vector = (Vector) value.get(0);
            Integer s = (Integer) value.get(1);
            Vector vector2 = new DenseVector(ArrayUtils.addAll(vector.toArray(), new double[]{s.doubleValue()}));
            return Arrays.toString(vector2.toArray())
                    .replace("[", "")
                    .replace("]", "")
                    .replaceAll(" ", "");
        });
        System.out.println(rr.first());
        // brak mozliwosci nadpisania, lepiej zrobic dataframe i wtedy zapisywac z mode overwrite
        rr.coalesce(1).saveAsTextFile("data/ok");
    }

    // Transform JavaRDD<DataModel> -> Dataset<Row>
    public static Dataset<Row> createDataSet(JavaRDD<DataModel> x, SparkSession spark, String featuresCol, String predictionCol) {
        JavaRDD<Row> ss = x.map(v1 -> RowFactory.create(v1.getInputData().toArray(), v1.getCluster()));
        // new StructType
        StructType schema = new StructType(new StructField[]{
                new StructField(featuresCol, new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),
                new StructField(predictionCol, DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dm = spark.createDataFrame(ss, schema);
        return dm;
    }

    // Transform JavaRDD<DataModel> -> Dataset<Row>  (VectorUDF)
    public static Dataset<Row> createDataSetUDF(JavaRDD<DataModel> x, SparkSession spark, String featuresCol, String predictionCol) {
        JavaRDD<Row> ss = x.map(v1 -> RowFactory.create(v1.getInputData(), v1.getCluster()));
        // new StructType
        StructType schema = new StructType(new StructField[]{
                new StructField(featuresCol, new VectorUDT(), false, Metadata.empty()),
                new StructField(predictionCol, DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dm = spark.createDataFrame(ss, schema);
        return dm;
    }

    public static JavaRDD<DataModel> computeDistances(JavaRDD<Vector> x, ArrayList<Vector> centers) {

        JavaRDD<DataModel> e = x.map(row -> {
            double[] distances = new double[centers.size()];
            for (int i = 0; i < centers.size(); i++) {
                distances[i] = distanceEuclidean(row.toArray(), centers.get(i).toArray());
                //distances[i] = distanceEuclidean2(row, centers.get(i));
            }
            DataModel dataModel = new DataModel(); // new DenseVector(distances);
            dataModel.setDistances(new DenseVector(distances));
            dataModel.setInputData(row);
            return dataModel;
        });
        return e;
    }

    public static JavaRDD<DataModel> predictCluster(JavaRDD<DataModel> x) {

        JavaRDD<DataModel> e = x.map(row -> {
            DataModel dataModel = new DataModel();
            dataModel.setCluster(findLowerValIndex(row.getDistances().toArray()));
            dataModel.setInputData(row.getInputData());
            dataModel.setDistances(row.getDistances());
            return dataModel;
        });
        return e;
    }

    public static double distanceEuclidean(double[] t1, double[] t2) {

        double sum = 0;
        for (int i = 0; i < t1.length; i++) {
            sum += Math.pow((t1[i] - t2[i]), 2.0);
        }
        return Math.sqrt(sum);
    }

    public static double distanceEuclidean2(Vector t1, Vector t2) {

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

    public static Vector mean(JavaRDD<DataModel> list, int index, Vector clusterCenter) {

        JavaRDD<DataModel> filtered = list.filter(v1 -> v1.getCluster() == index);
        long count = filtered.count();
        System.out.println("COUNT INDEX: " + index + " , =" + count);
//        filtered
//                .map(v1 -> v1.getInputData().toArray())
//                .foreach(doubles -> System.out.println(Arrays.toString(doubles)));

        //double[] mm = new double[clusterCenter.size()];
        Vector mm;
        if (count == 0) {
            mm = new DenseVector(clusterCenter.toArray());
        }
//        else if (count == -1) {
//            // Ten warunek mozna pominac, reduce dla 1 rekordu tez liczy dobrze
//            //System.out.println(Arrays.toString(filtered.take(1).get(0).getInputData().toArray()));
//            mm = new DenseVector(filtered.take(1).get(0).getInputData().toArray());
//        }
        else {
//            if (count>0){
//                for (int i = 0; i < count ; i++) {
//                    System.out.println(Arrays.toString(filtered.take((int) count).get(i).getInputData().toArray()));
//                }
//            }
            mm = filtered
                    .map(v1 -> v1.getInputData())
                    .reduce((v1, v2) -> sumArrayByColumn(v1, v2));

            double[] mm2 = mm.toArray();
            for (int i = 0; i < mm2.length; i++) {
                mm2[i] /= count;
            }
            mm = new DenseVector(mm2);
        }
        return mm;
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

    public static class DataModel implements Serializable{
        private Vector inputData;
        private Vector distances;
        private int cluster;

        public Vector getInputData() {
            return inputData;
        }

        public void setInputData(Vector inputData) {
            this.inputData = inputData;
        }

        public Vector getDistances() {
            return distances;
        }

        public void setDistances(Vector distances) {
            this.distances = distances;
        }

        public int getCluster() {
            return cluster;
        }

        public void setCluster(int cluster) {
            this.cluster = cluster;
        }
    }
}


//    public static JavaRDD<Vector> convertToRDD(Dataset<Row> ds) {
//        // Convert dataset to JavaRDD of Vectors
//        JavaRDD<Vector> x3 = ds.toJavaRDD().map(row -> {
//            return (Vector) row.get(0);
////            double[] array = new double[row.size()];
////            for (int i = 0; i < row.size(); i++) {
////                array[i] = row.getDouble(i);
////            }
////            return new DenseVector(array);
//        });
//        return x3;
//    }

//   System.out.println("Starting centers:" + Arrays.toString(clusterCenters.toArray()));
//
//           for (int m = 0; m < 1; m++) {
//
//        // Compute distances
//        JavaRDD<DataModel> x4 = computeDistances(x3, clusterCenters);
//        // Predict Cluster
//        JavaRDD<DataModel> x5 = predictCluster(x4);
//
//        //System.out.println("\nPREDICTION FIT");
//        //x5.foreach(vector -> System.out.println(vector.getCluster() + "  " + Arrays.toString(vector.getInputData().toArray())));
//
//        // Update centers
//        for (int i = 0; i < clusterCenters.size(); i++) {
//        clusterCenters.set(i, new DenseVector(mean(x5, i)));
//        }
//        }
//        System.out.println("Finish centers:" + Arrays.toString(clusterCenters.toArray()));