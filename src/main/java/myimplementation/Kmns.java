package myimplementation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.clustering.ClusteringSummary;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.*;
import org.apache.spark.ml.linalg.BLAS$;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.clustering.VectorWithNorm;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.util.MLUtils$;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

import java.io.Serializable;
import java.util.*;

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
        //String path = "data/mllib/iris2.csv";
        //String path = "data/mllib/creditcard.csv";

        //String path = "data/mllib/serce.csv";
        //String path = "data/mllib/rezygnacje.csv";
        //String path = "data/mllib/rezygnacje.csv";
        //String path = "data/mllib/sat.csv"; // PROBLEM Z FORMATEM DANYCH

        //String path = "data/mllib/creditcardBIG.csv";
        //String path = "hdfs:/192.168.100.4/data/mllib/kmean.txt";

        // load mem data
        MemDataSet memDataSet = new MemDataSet(spark);
        memDataSet.loadDataSet(path);
        //memDataSet.getDs().cache();
        ///Dataset<Row> ds2 = memDataSet.getDs();//DataPrepare.fillMissingValues(memDataSet.getDs()); //memDataSet.getDs();
        DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> ds1 = dpc.prepareDataSet(memDataSet.getDs(), false, true);
        Dataset<Row> ds = ds1.select("features"); //normFeatures //features
        //ds.show(false);
//        ds.printSchema();

        ///////////////////////////////////////////////////////////////////
//        JavaRDD<Row> filteredRDD = ds
//                .toJavaRDD()
//                .zipWithIndex()
//                // .filter((Tuple2<Row,Long> v1) -> v1._2 >= start && v1._2 < end)
//                .filter((Tuple2<Row,Long> v1) ->
//                        v1._2==1 || v1._2==2 || v1._2==22 || v1._2==100 || v1._2==222 || v1._2==2000 || v1._2==7000 || v1._2==5000 || v1._2==3000 || v1._2==666)
//                .map(r -> r._1);
//
//
//        List<Vector> cx = filteredRDD.map(v -> (Vector)v.get(0)).collect();
//
//        ArrayList<Vector> newCenters = new ArrayList<>();
//        newCenters.addAll(cx);
//
//        System.out.println(newCenters.toString());
//
//        ArrayList<Vector> clusterCenters = newCenters;
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Convert dataset to JavaRDD of Vectors
        //JavaRDD<Vector> x3 = convertToRDD(ds);
        //JavaRDD<DataModel> x3 = convertToRDDModel(ds);

        //JavaPairRDD<Integer, Vector> x3 = Util.convertToRDDModel2(ds);
        //JavaRDD<Vector> x3 = Util.convertToRDDModel4(ds);
        JavaRDD<DataModel> x3 = Util.convertToRDDModel(ds);

        if (x3.getStorageLevel() == StorageLevel.NONE()) {
            System.out.println("NONE :::::::" + x3.getStorageLevel().toString());
        } else {
            System.out.println("NOT NONE");
        }
        System.out.println("XXXX12312312323:  " + x3.getStorageLevel().toString());
        //x3.persist(StorageLevel.MEMORY_ONLY());
        System.out.println("XXXX12312312323:  " + x3.getStorageLevel().toString());

        int k = 4;
//        ArrayList<DataModel> cc = new ArrayList<>(x3.takeSample(false, k, 20L));
////      ArrayList<Tuple2<Integer, Vector>> cc = new ArrayList<>(x3.takeSample(false, k, 20L));
//        ArrayList<Vector> clusterCenters = new ArrayList<>();
////        for (Tuple2<Integer, Vector> dm : cc) {
////            clusterCenters.add(dm._2());
////        }
//        for (DataModel dm : cc) {
//            clusterCenters.add(dm.getData());
//        }

        //ArrayList<Vector> clusterCenters = new ArrayList<>(x3.takeSample(false, k, 20L));


//        ArrayList<Vector> clusterCenters = new ArrayList<>();
//        clusterCenters.add(new DenseVector(new double[]{5.1,3.5,1.4,0.2}));
//        clusterCenters.add(new DenseVector(new double[]{5.7,3.8,1.7,0.3}));
        //        System.out.println("Starting centers:" + Arrays.toString(clusterCenters.toArray()));
        //

        ArrayList<Vector> cc = initializeCenters(x3, k);

        ArrayList<Vector> clusterCenters2 = computeCenters(x3, cc, 1e-4, 10);
        //        x3.unpersist();


        ////////////////////////////////////////////////////////////////////////////////////////
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

    public static ArrayList<Vector> initializeCenters(JavaRDD<DataModel> data, int k) {
        ArrayList<DataModel> initialCenters = new ArrayList<>(data.takeSample(false, k, 20L));
        ArrayList<Vector> initialCentersVector = new ArrayList<>();
        for (DataModel dataModel : initialCenters) {
            initialCentersVector.add(dataModel.getData());
        }
        return initialCentersVector;
    }

    public static ArrayList<Vector> computeCenters(JavaRDD<DataModel> data, ArrayList<Vector> centers, double epsilon, int maxIterations) {

        JavaSparkContext jsc = new JavaSparkContext(data.context());
        LongAccumulator accumulator = jsc.sc().longAccumulator("Accumulator_1");
        ArrayList<Vector> clusterCenters = new ArrayList<>(centers);
        // double epsilon = 1e-4;
        // int maxIterations = 20;
        boolean bol = true;
        int ii = 0;

        do {
            data.persist(StorageLevel.MEMORY_ONLY());
            long startTime = System.currentTimeMillis();

            ArrayList<Vector> newClusterCenters = new ArrayList<>(clusterCenters);

            // 1
            //JavaPairRDD<Integer, Vector> s1 = predictClusterWithNorm(data, newClusterCenters);
            JavaPairRDD<Integer, Vector> s1 = predictCluster(data, newClusterCenters);

            // 2
            JavaPairRDD<Integer, Tuple2<Long, Vector>> s2 = s1.mapPartitionsToPair(t -> {
                List<Tuple2<Integer, Tuple2<Long, Vector>>> list = new ArrayList<>();
                while (t.hasNext()) {
                    //DataModel element = t.next();
                    Tuple2<Integer, Vector> element = t.next();
                    //System.out.println(element.getCluster()+"__"+Arrays.toString(element.getData().toArray()));
                    //list.add(new Tuple2<>(element.getCluster(), new Tuple2<>(1L, element.getData())));
                    list.add(new Tuple2<>(element._1(), new Tuple2<>(1L, element._2())));
                }
                return list.iterator();
            });

            // 3
            JavaPairRDD<Integer, Tuple2<Long, Vector>> s3 = s2.reduceByKey((v1, v2) -> {

                DenseVector dd = v1._2().toDense();//.copy();
                BLAS$.MODULE$.axpy(1.0, v2._2(), dd);
                return new Tuple2<>(v1._1() + v2._1(), dd);


                //return new Tuple2<>(v1._1() + v2._1(), Util.sumArrayByColumn(v1._2(), v2._2()));

                //  return new Tuple2<>(v1._1() + v2._1(), new FastAxpy().axpy(1.0, v2._2(), v1._2().toDense()));
            });

            // 4
            JavaPairRDD<Integer, Vector> s4 = s3.mapValues(v1 -> {
                Vector v = v1._2();
                BLAS$.MODULE$.scal(1.0 / v1._1(), v);
                return v;
//                Vector v = Util.divideArray(v1._2(), v1._1());
//                return v;
            });

            // 5
            Map<Integer, Vector> xc = s4.collectAsMap();

            data.unpersist();


//            Map<Integer, Vector> xc2 = predictClusterWithNorm(data, newClusterCenters)
//                    .mapToPair(t -> new Tuple2<>(t._1(), new Tuple2<>(1L, t._2())))
//                    .reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1(), Util.sumArrayByColumn(v1._2(), v2._2())))
//                    .mapValues(v1 -> Util.divideArray(v1._2(), v1._1()))
//                    .collectAsMap();


            ////////////////////////////////////////
            long endTime = System.currentTimeMillis();
            accumulator.add(endTime - startTime);
            ///////////////////////////////////////

            double centersDistance = 0.0;

            for (int i = 0; i < clusterCenters.size(); i++) {
                Vector tp = xc.get(i);
                //System.out.println(Arrays.toString(tp.toArray()));
                if (tp != null) {
                    newClusterCenters.set(i, tp);
                } else {
                    newClusterCenters.set(i, newClusterCenters.get(i));
                }
                //centersDistance += org.apache.spark.ml.linalg.Vectors.sqdist(clusterCenters.get(i), newClusterCenters.get(i)); //4,2s
                //centersDistance += Distances.squaredDistance(clusterCenters.get(i), newClusterCenters.get(i)); // 3,7s
                centersDistance += Distances.squaredDistance2(clusterCenters.get(i).toArray(), newClusterCenters.get(i).toArray()); // 4s
            }
            centersDistance = centersDistance / clusterCenters.size();

            if (centersDistance < epsilon || ii == maxIterations - 1) {
                bol = false;
            } else {
                clusterCenters = new ArrayList<>(newClusterCenters);
                ii++;
                //System.out.println("ITERATION: " + ii);
                System.out.println("ITERATION: " + ii + ", ACCUMULATOR: " + accumulator.value() + " ms");
            }
        } while (bol);

        return clusterCenters;
    }

    public static JavaPairRDD<Integer, Vector> predictCluster(JavaRDD<DataModel> data, ArrayList<Vector> centers) {

        JavaSparkContext jsc = new JavaSparkContext(data.context());

        Broadcast<ArrayList<Vector>> centersBroadcast = jsc.broadcast(centers);

        JavaPairRDD<Integer, Vector> predictedClusters = data
                .mapPartitionsToPair(dataModel -> {
                    List<Tuple2<Integer, Vector>> list = new ArrayList<>();
                    while (dataModel.hasNext()) {
                        Vector points = dataModel.next().getData();
                        double[] distances = computeDistance(centersBroadcast.value(), points);
                        int predictedCluster = Util.findLowerValIndex(distances);
                        list.add(new Tuple2<>(predictedCluster, points));
                    }
                    return list.iterator();
                });

        centersBroadcast.unpersist(false);
        return predictedClusters;
    }

    private static double[] computeDistance(ArrayList<Vector> centers, Vector point) {
        double[] distances = new double[centers.size()];
        for (int i = 0; i < centers.size(); i++) {

            // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            //double d = Distances.distanceEuclidean2(point, centers.get(i)); // 7s !!!!!!!!!!!!!!
            double d = Vectors.sqdist(point, centers.get(i)); //3,7s - 4,2s


            //double d = Distances.distanceEuclidean(point.toArray(), centers.get(i).toArray()); //4s -3,9

            // double d = Distances.fastDistanceXD(point.vector(), centers.get(i).vector());

            // VECTOR WITH NORM//////////////////////////////////////////////////////// 4,7s
//            ArrayList<VectorWithNorm> centers2 = new ArrayList<>();
//            for (Vector v : centers) {
//                centers2.add(new VectorWithNorm(org.apache.spark.mllib.linalg.Vectors.fromML(v)));
//            }
//            VectorWithNorm point2 = new VectorWithNorm(org.apache.spark.mllib.linalg.Vectors.fromML(point),2.0);
//            double d = MLUtils$.MODULE$.fastSquaredDistance(
//                    point2.vector(), point2.norm(),
//                    centers2.get(i).vector(), centers2.get(i).norm(),
//                    1e-6);
            ////////////////////////////////////////////////////////////////////////////////////////

//            double d = Distances.fastSquaredDistance(point2.vector(), point2.norm(),  // 4,3s
//                    centers2.get(i).vector(), centers2.get(i).norm());

//            double d = Distances.fastSquaredDistance_V2(point2.vector(), point2.norm(), //4.1s
//                    centers2.get(i).vector(), centers2.get(i).norm());

            distances[i] = d;
        }
        return distances;
    }

}