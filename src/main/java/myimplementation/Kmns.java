package myimplementation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.clustering.ClusteringSummary;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.BLAS$;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.clustering.VectorWithNorm;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.util.MLUtils$;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

import java.io.Serializable;
import java.util.*;

import static myimplementation.Util.saveAsCSV;

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
                .setAppName("KMeans_Implementation")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                .set("spark.driver.memory", "2g")
                .set("spark.executor.memory", "2g")
                .setMaster("local[*]");



//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_MY_implementation")
//                .setMaster("spark://10.2.28.17:7077")
//                .set("spark.eventLog.dir", "file:///C:/logs")
//                .set("spark.eventLog.enabled", "true")
//                .setJars(new String[]{"out/artifacts/SparkProject_jar/SparkProject.jar"})
//                .set("spark.executor.memory", "5g")
//                //.set("spark.cores.max", "1")
//                //.set("spark.default.parallelism", "12")
//                .set("spark.driver.host", "10.2.28.31");

        SparkContext sc = new SparkContext(conf);
        SparkSession spark = new SparkSession(sc);
        //JavaSparkContext jsc = new JavaSparkContext(sc);

        System.out.println("**********" + sc.defaultParallelism() + "  ," + sc.defaultMinPartitions());

        //String path = "hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt";

        //String path = "hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt.gz";
        //String path = "hdfs://192.168.100.4:9000/spark/kdd_10_proc.txt.gz";
        //String path = "data/mllib/kdd_10_proc.txt";
        //String path = "data/mllib/kdd_5_proc.txt";
        String path = "data/mllib/kdd_3_proc.txt";
        //String path = "data/mllib/flights_low.csv";
        //String path = "data/mllib/kddFIX.txt";
        //String path = "data/mllib/kddcup_train.txt";
        //String path = "data/mllib/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup.txt";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kmean.txt";
        //String path = "data/mllib/kmean.txt";
        //String path = "data/mllib/kmean.txt";
        //String path = "data/mllib/iris2.csv";
        //String path = "data/mllib/creditcard.csv";
        //String path = "data/mllib/creditcardBIG.csv";
        //String path = "hdfs:/192.168.100.4/data/mllib/kmean.txt";

        // load mem data
        MemDataSet memDataSet = new MemDataSet(spark);
        memDataSet.loadDataSet(path);
        //memDataSet.getDs().cache();
        ///Dataset<Row> ds2 = memDataSet.getDs();//DataPrepare.fillMissingValues(memDataSet.getDs()); //memDataSet.getDs();
        DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> ds1 = dpc.prepareDataset(memDataSet.getDs(), false, true);
        Dataset<Row> ds = ds1.select("features"); //normFeatures //features
        //ds.show(false);
//        ds.printSchema();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Convert dataset to JavaRDD of Vectors
        //JavaRDD<Vector> x3 = convertToRDD(ds);
        //JavaRDD<DataModel> x3 = convertToRDDModel(ds);
        JavaPairRDD<Integer, Vector> x3 = Util.convertToRDDModel2(ds);
        if (x3.getStorageLevel() == StorageLevel.NONE()) {
            System.out.println("NONE :::::::" + x3.getStorageLevel().toString());
        } else {
            System.out.println("NOT NONE");
        }
        System.out.println("XXXX12312312323:  " + x3.getStorageLevel().toString());
        //x3.persist(StorageLevel.MEMORY_ONLY());
        System.out.println("XXXX12312312323:  " + x3.getStorageLevel().toString());

        int k = 3;
        //ArrayList<DataModel> cc = new ArrayList<>(x3.takeSample(false, k, 20L));
        ArrayList<Tuple2<Integer, Vector>> cc = new ArrayList<>(x3.takeSample(false, k, 20L));
        ArrayList<Vector> clusterCenters = new ArrayList<>();
        for (Tuple2<Integer, Vector> dm : cc) {
            clusterCenters.add(dm._2());
        }

        //ArrayList<Vector> clusterCenters = new ArrayList<>();
        //clusterCenters.add(new DenseVector(new double[]{4.8,3.0,1.4,0.3,0.0,1.0}));
        //clusterCenters.add(new DenseVector(new double[]{6.9,3.1,4.9,1.5,1.0,0.0}));
        System.out.println("Starting centers:" + Arrays.toString(clusterCenters.toArray()));

        ArrayList<Vector> clusterCenters2 = computeCenters(x3, clusterCenters);
        x3.unpersist();

        // Compute distances, Predict Cluster
        //JavaRDD<DataModel> x5 = predictAll(x3, clusterCenters2);
        JavaPairRDD<Integer, Vector> x5 = predictCluster(x3, clusterCenters2);

        Dataset<Row> dm = Util.createDataSet2(x5, spark, "features", "prediction");
        dm.cache();
        dm.show();
        //dm.printSchema();

        Util.printCenters(clusterCenters2);

        ClusteringEvaluator clusteringEvaluator = new ClusteringEvaluator();
        clusteringEvaluator.setFeaturesCol("features");
        clusteringEvaluator.setPredictionCol("prediction");
        System.out.println("EVAL: " + clusteringEvaluator.evaluate(dm));

        ClusteringSummary clusteringSummary = new ClusteringSummary(dm, "prediction", "features", k);
        System.out.println(Arrays.toString(clusteringSummary.clusterSizes()));
        dm.unpersist();

        //Util.saveAsCSV(dm);
        //new Scanner(System.in).nextLine();
        spark.close();
    }

    public static ArrayList<Vector> computeCenters(JavaPairRDD<Integer, Vector> x33, ArrayList<Vector> cc) {

        JavaSparkContext jsc = new JavaSparkContext(x33.context());
        org.apache.spark.util.LongAccumulator accum = jsc.sc().longAccumulator("Accumulator_1");
        ArrayList<Vector> clusterCenters = new ArrayList<>(cc);
        double epsilon = 1e-4;
        int max_iter = 20;
        boolean bol = true;
        int ii = 0;

        do {
            x33.persist(StorageLevel.MEMORY_ONLY());
            long startTime = System.currentTimeMillis();

            ArrayList<Vector> clusterCenters2 = new ArrayList<>(clusterCenters);

            // 1
            JavaPairRDD<Integer, Vector> s1 = predictCluster(x33, clusterCenters2);

            // 2
            JavaPairRDD<Integer, Tuple2<Long, Vector>> s2 = s1.mapPartitionsToPair(t -> {
                List<Tuple2<Integer, Tuple2<Long, Vector>>> list = new ArrayList<>();
                while (t.hasNext()) {
                    //DataModel element = t.next();
                    Tuple2<Integer, Vector> element = t.next();
                    //System.out.println(element.getCluster()+"__"+Arrays.toString(element.getInputData().toArray()));
                    //list.add(new Tuple2<>(element.getCluster(), new Tuple2<>(1L, element.getInputData())));
                    list.add(new Tuple2<>(element._1(), new Tuple2<>(1L, element._2())));
                }
                return list.iterator();
            });

            // 3
            JavaPairRDD<Integer, Tuple2<Long, Vector>> s3 = s2.reduceByKey((v1, v2) -> {
//                  BLAS$.MODULE$.axpy(1.0, v2._2().toDense(), v1._2().toDense());
//                  return new Tuple2<>(v1._1() + v2._1(), v1._2());
               return new Tuple2<>(v1._1() + v2._1(), Util.sumArrayByColumn(v1._2(), v2._2()));
            });

            // 4
            JavaPairRDD<Integer, Vector> s4 = s3.mapValues(v1 -> {
//                Vector v = new DenseVector(v1._2().toArray());
//                BLAS$.MODULE$.scal(1.0 / v1._1(), v);
//                return v;
                Vector v = Util.divideArray(v1._2(), v1._1());
                return v;
            });

            // 5
            Map<Integer, Vector> xc = s4.collectAsMap();

            x33.unpersist();


//            Map<Integer, Vector> xc2 = predictCluster(x33, clusterCenters2)
//                    .mapToPair(t -> new Tuple2<>(t._1(), new Tuple2<>(1L, t._2())))
//                    .reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1(), Util.sumArrayByColumn(v1._2(), v2._2())))
//                    .mapValues(v1 -> Util.divideArray(v1._2(), v1._1()))
//                    .collectAsMap();


            ////////////////////////////////////////
            long endTime = System.currentTimeMillis();
            accum.add(endTime - startTime);
            ///////////////////////////////////////

            double centers_distance = 0.0;

            for (int i = 0; i < clusterCenters.size(); i++) {
                Vector tp = xc.get(i);
                //System.out.println(Arrays.toString(tp.toArray()));
                if (tp != null) {
                    clusterCenters2.set(i, tp);
                } else {
                    clusterCenters2.set(i, clusterCenters2.get(i));
                }
                centers_distance += org.apache.spark.ml.linalg.Vectors.sqdist(clusterCenters.get(i), clusterCenters2.get(i));
            }
            centers_distance = centers_distance / clusterCenters.size();

            if (centers_distance < epsilon || ii == max_iter - 1) {
                bol = false;
            } else {
                clusterCenters = new ArrayList<>(clusterCenters2);
                ii++;
                System.out.println("ITERATION: " + ii);
            }
            System.out.println("ACCUMULATOR: " + accum.value());
        } while (bol);

        return clusterCenters;
    }


    public static JavaPairRDD<Integer, Vector> predictCluster(JavaPairRDD<Integer, Vector> x, ArrayList<Vector> cc) {

        JavaSparkContext jsc = new JavaSparkContext(x.context());

        ArrayList<VectorWithNorm> ww = new ArrayList<>();
        for (Vector v : cc) {
            ww.add(new VectorWithNorm(Vectors.fromML(v)));
        }

        Broadcast<ArrayList<VectorWithNorm>> ccc = jsc.broadcast(ww);

        JavaPairRDD<Integer, Vector> e = x
                .mapPartitionsToPair(vvv -> {
                    List<Tuple2<Integer, Vector>> list = new ArrayList<>();
                    while (vvv.hasNext()) {
                        Tuple2<Integer, Vector> v1 = vvv.next();
                        double[] dd = computeDistance(ccc.value(), new VectorWithNorm(Vectors.fromML(v1._2())));
                        int index = Util.findLowerValIndex(dd);
                       // System.out.println(index+", "+Arrays.toString(v1._2().toArray()));
                        list.add(new Tuple2<>(index, v1._2()));
                    }
                    return list.iterator();
                });

        ccc.unpersist(false);
        return e;
    }

    public static double[] computeDistance(ArrayList<VectorWithNorm> centers, VectorWithNorm point) {
        double[] dd = new double[centers.size()];
        for (int i = 0; i < centers.size(); i++) {

//            double d = MLUtils$.MODULE$.fastSquaredDistance(
//                    point.vector(), point.norm(),
//                    centers.get(i).vector(), centers.get(i).norm(),
//                    1e-6);

            double d = Distances.fastSquaredDistance(point.vector(), point.norm(),
                    centers.get(i).vector(), centers.get(i).norm());

            //double d = Distances.distanceEuclidean2(point.vector(), centers.get(i).vector());
            dd[i] = d;
        }
        return dd;
    }


    public static class DataModel implements Serializable {
        private Vector inputData;
        private int cluster;

        public Vector getInputData() {
            return inputData;
        }

        public DataModel setInputData(Vector inputData) {
            this.inputData = inputData;
            return this;
        }

        public int getCluster() {
            return cluster;
        }

        public DataModel setCluster(int cluster) {
            this.cluster = cluster;
            return this;
        }
    }
}