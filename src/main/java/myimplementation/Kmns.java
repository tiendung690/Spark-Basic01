package myimplementation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

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
//        Logger.getLogger("org").setLevel(Level.OFF);
//        Logger.getLogger("akka").setLevel(Level.OFF);
//        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Spark_Experiment_Implementation_Kmeans")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                //.set("spark.driver.memory", "4g")
                //.set("spark.executor.memory", "4g")
                .setMaster("local");

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
        String path = "data/mllib/kdd_10_proc.txt.gz";
        //String path = "data/mllib/kddcup_train.txt";
        //String path = "data/mllib/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup.txt";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kmean.txt";
        //String path = "data/mllib/kmean.txt";
        //String path = "data/mllib/iris.csv";
        //String path = "hdfs:/192.168.100.4/data/mllib/kmean.txt";

        // load mem data
        MemDataSet memDataSet = new MemDataSet(spark);
        memDataSet.loadDataSet(path);

        DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> ds = dpc.prepareDataset(memDataSet.getDs(), false).select("features");
        ds.show();
        ds.printSchema();


//        JavaRDD<Row> x1 = ds.toJavaRDD();
//        JavaRDD<String> x2 = ds.toJavaRDD().map(value -> String.valueOf(value.get(0)));
//        System.out.println(x2.take(5));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Convert dataset to JavaRDD of Vectors
        JavaRDD<Vector> x3 = ds.toJavaRDD().map(row -> {


            return (Vector) row.get(0);
//            double[] array = new double[row.size()];
//            for (int i = 0; i < row.size(); i++) {
//                array[i] = row.getDouble(i);
//            }
//            return new DenseVector(array);
        });

        //System.out.println("NUM PARTITIONS: " + x33.getNumPartitions());
        //JavaRDD<Vector> x3 = x33.repartition(4);
        x3.cache();
        //System.out.println("NUM PARTITIONS: " + x3.getNumPartitions());

        // Take starting points
        //List<Vector> startingCenters = x3.takeSample(false, 2);
        ArrayList<Vector> clusterCenters = new ArrayList<>(x3.takeSample(false, 2));

//        ArrayList<Vector> clusterCenters = new ArrayList<>();
//        clusterCenters.add(new DenseVector(new double[]{2.0,2.0}));
//        clusterCenters.add(new DenseVector(new double[]{4.0,6.0}));

        System.out.println("Starting centers:" + Arrays.toString(clusterCenters.toArray()));

        for (int m = 0; m < 1; m++) {

            // Compute distances
            JavaRDD<DataModel> x4 = computeDistances(x3, clusterCenters);
            // Predict Cluster
            JavaRDD<DataModel> x5 = predictCluster(x4);

            //System.out.println("\nPREDICTION FIT");
            //x5.foreach(vector -> System.out.println(vector.getCluster() + "  " + Arrays.toString(vector.getInputData().toArray())));

            // Update centers
            for (int i = 0; i < clusterCenters.size(); i++) {
                clusterCenters.set(i, new DenseVector(mean(x5, i)));
            }
        }
        System.out.println("Finish centers:" + Arrays.toString(clusterCenters.toArray()));


        // Compute distances
        JavaRDD<DataModel> x4 = computeDistances(x3, clusterCenters);
        // Predict Cluster
        JavaRDD<DataModel> x5 = predictCluster(x4);
//        System.out.println("\nPREDICTION TRANSOFORM");
//        x5.foreach(vector -> System.out.println(vector.getCluster()+"  "+Arrays.toString(vector.getInputData().toArray())));
        Dataset<Row> dm = createDataSet(x5, spark);
        dm.show();
        dm.printSchema();


        //new Scanner(System.in).nextLine();
        spark.close();
    }


    // Transform JavaRDD<DataModel> -> Dataset<Row>
    public static Dataset<Row> createDataSet(JavaRDD<DataModel> x, SparkSession spark) {
        JavaRDD<Row> ss = x.map(v1 -> RowFactory.create(v1.getInputData().toArray(), v1.getCluster()));
        // new StructType
        StructType schema = new StructType(new StructField[]{
                new StructField("values", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),
                new StructField("cluster", DataTypes.DoubleType, true, Metadata.empty())
        });
        Dataset<Row> dm = spark.createDataFrame(ss, schema);
        return dm;
    }

    public static JavaRDD<DataModel> computeDistances(JavaRDD<Vector> x, ArrayList<Vector> centers) {

        JavaRDD<DataModel> e = x.map(row -> {
            double[] distances = new double[centers.size()];
            for (int i = 0; i < centers.size(); i++) {
                distances[i] = distanceChebyshev(row.toArray(), centers.get(i).toArray());
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
            //return new LabeledPoint(findLowerValIndex(row.toArray()), row);
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


    public static double findLowerValIndex(double[] tab) {

        int index = 0;
        double min = tab[index];
        for (int i = 1; i < tab.length; i++) {
            if (tab[i] < min) {
                min = tab[i];
                index = i;
            }
        }
        return (double) index;
    }

    public static double[] mean(JavaRDD<DataModel> list, int index) {

        JavaRDD<DataModel> filtered = list.filter(v1 -> v1.getCluster() == index);
        long count = filtered.count();

//        double[] mm = list.filter(v1 -> v1.getCluster() == index)
//                .map(v1 -> v1.getInputData().toArray())
//                .reduce((v1, v2) -> sumArrayByColumn(v1, v2));

        double[] mm = filtered
                .map(v1 -> v1.getInputData().toArray())
                .reduce((v1, v2) -> sumArrayByColumn(v1, v2));

        //System.out.println("MM: "+Arrays.toString(mm));

        for (int i = 0; i < mm.length; i++) {
            mm[i] /= count;   // CHANGE
        }

//        for (int i = 0; i < mm.length; i++) {
//            mm[i] /= list.filter(v1 -> v1.getCluster() == index).count();   // it may be slow, NEED CHANGE
//        }

        //  System.out.println("CENTROID: " + index + ", :" + Arrays.toString(mm));

        return mm;
    }

    public static double[] sumArrayByColumn(double[] t1, double[] t2) {
        double[] tab = new double[t1.length];
        for (int i = 0; i < t1.length; i++) {
            tab[i] = t1[i] + t2[i];
        }
        return tab;
    }

    public static class DataModel {
        private Vector inputData;
        private Vector distances;
        private double cluster;

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

        public double getCluster() {
            return cluster;
        }

        public void setCluster(double cluster) {
            this.cluster = cluster;
        }
    }
}
