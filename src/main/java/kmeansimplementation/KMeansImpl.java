package kmeansimplementation;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by as on 09.04.2018.
 */
public class KMeansImpl {

    // Logger.
    public static final String loggerName = "K-Means Implementation";
    private static final Logger logger = Logger.getLogger(loggerName);

    public static ArrayList<Vector> initializeCenters(JavaRDD<DataModel> data, int k) {
        ArrayList<DataModel> initialCenters = new ArrayList<>(data.takeSample(false, k, 20L));
        ArrayList<Vector> initialCentersVector = new ArrayList<>();
        for (DataModel dataModel : initialCenters) {
            initialCentersVector.add(dataModel.getData());
        }
        return initialCentersVector;
    }

    private static Map<Integer, Vector> predictClusterAndComputeNewCenters2(JavaRDD<DataModel> data, ArrayList<Vector> clusterCenters) {
        // 1
        JavaPairRDD<Integer, Vector> s1 = predictCluster2(data, clusterCenters);
        // 2
        JavaPairRDD<Integer, Tuple2<Long, Vector>> s2 = s1.mapPartitionsToPair(t -> {
            List<Tuple2<Integer, Tuple2<Long, Vector>>> list = new ArrayList<>();
            while (t.hasNext()) {
                Tuple2<Integer, Vector> element = t.next();
                list.add(new Tuple2<>(element._1(), new Tuple2<>(1L, element._2())));
            }
            return list.iterator();
        });
        // 3
        JavaPairRDD<Integer, Tuple2<Long, Vector>> s3 = s2.reduceByKey((v1, v2) -> {
            //DenseVector dd = v1._2().toDense();
            //BLAS$.MODULE$.axpy(1.0, v2._2(), dd);
            //return new Tuple2<>(v1._1() + v2._1(), dd);
            return new Tuple2<>(v1._1() + v2._1(), sumArrayByColumn(v1._2().toArray(), v2._2().toArray()));
        });
        // 4
        JavaPairRDD<Integer, Vector> s4 = s3.mapValues(v1 -> {
            //Vector v = v1._2();
            //BLAS$.MODULE$.scal(1.0 / v1._1(), v);
            //return v;
            return divideArray(v1._2().toArray(), v1._1());
        });
        // 5
        Map<Integer, Vector> newCenters = s4.collectAsMap();
        return newCenters;
    }

    private static Map<Integer, Vector> predictClusterAndComputeNewCenters(JavaRDD<DataModel> data, ArrayList<Vector> clusterCenters) {
        Map<Integer, Vector> newCenters = predictCluster(data, clusterCenters)                                   // 1
                .mapToPair(t -> new Tuple2<>(t._1(), new Tuple2<>(1L, t._2())))                               // 2
                .reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1(), sumArrayByColumn(v1._2().toArray(), v2._2().toArray()))) // 3
                .mapValues(v1 -> divideArray(v1._2().toArray(), v1._1()))                                                   // 4
                //.map(v1 -> new Tuple2<>(v1._1(), divideArray(v1._2()._2(), v1._2._1()))) // 4
                .collectAsMap();                                                                                  // 5
        return newCenters;

    }

    public static ArrayList<Vector> computeCenters(JavaRDD<DataModel> data, ArrayList<Vector> centers, double epsilon, int maxIterations) {

        JavaSparkContext jsc = new JavaSparkContext(data.context());
        LongAccumulator accumulator = jsc.sc().longAccumulator("K-means_Accumulator");
        ArrayList<Vector> clusterCenters = new ArrayList<>(centers);
        boolean condition = true;
        int iteration = 0;

        do {
            // Accumulator time.
            long startTime = System.currentTimeMillis();

            ArrayList<Vector> newClusterCenters = new ArrayList<>(clusterCenters);

            // 1. Predict testcluster.
            // 2. MapToPair.
            // 3. ReduceByKey.
            // 4. MapValues.
            // 5. CollectAsMap.
            data.persist(StorageLevel.MEMORY_ONLY());
            Map<Integer, Vector> newCenters = predictClusterAndComputeNewCenters(data, newClusterCenters);
            data.unpersist();


            // Accumulator time.
            long endTime = System.currentTimeMillis();
            accumulator.add(endTime - startTime);

            double centersDistance = 0.0;
            for (int i = 0; i < clusterCenters.size(); i++) {
                Vector tp = newCenters.get(i);
                if (tp != null) {
                    newClusterCenters.set(i, tp);
                } else {
                    newClusterCenters.set(i, newClusterCenters.get(i));
                }
                centersDistance += Distances.distanceSquared(clusterCenters.get(i).toArray(), newClusterCenters.get(i).toArray());
            }
            centersDistance = centersDistance / clusterCenters.size();
            if (centersDistance < epsilon || iteration == maxIterations) {
                condition = false;
                logger.info("Iteration: " + iteration + ", Accumulator: " + accumulator.value() + " ms");
                logger.info("Coveraged in "+iteration+" iterations.");
            } else {
                clusterCenters = new ArrayList<>(newClusterCenters);
                logger.info("Iteration: " + iteration + ", Accumulator: " + accumulator.value() + " ms");
            }
            iteration++;
        } while (condition);

        return clusterCenters;
    }

    public static JavaPairRDD<Integer, Vector> predictCluster2(JavaRDD<DataModel> data, ArrayList<Vector> centers) {

        JavaSparkContext jsc = new JavaSparkContext(data.context());
        Broadcast<ArrayList<Vector>> centersBroadcast = jsc.broadcast(centers);

        JavaPairRDD<Integer, Vector> predictedClusters = data
                .mapPartitionsToPair(dataModel -> {
                    List<Tuple2<Integer, Vector>> list = new ArrayList<>();
                    while (dataModel.hasNext()) {
                        Vector points = dataModel.next().getData();
                        double[] distances = computeDistance(centersBroadcast.value(), points);
                        int predictedCluster = findLowerValIndex(distances);
                        list.add(new Tuple2<>(predictedCluster, points));
                    }
                    return list.iterator();
                });

        centersBroadcast.unpersist(false);
        return predictedClusters;
    }

    public static JavaPairRDD<Integer, Vector> predictCluster(JavaRDD<DataModel> data, ArrayList<Vector> centers) {

        JavaSparkContext jsc = new JavaSparkContext(data.context());
        Broadcast<ArrayList<Vector>> centersBroadcast = jsc.broadcast(centers);

        JavaPairRDD<Integer, Vector> predictedClusters = data
                .mapToPair(dataModel -> {
                    Vector points = dataModel.getData();
                    double[] distances = computeDistance(centersBroadcast.value(), points);
                    int predictedCluster = findLowerValIndex(distances);
                    return new Tuple2<>(predictedCluster, points);
                });

        centersBroadcast.unpersist(false);
        return predictedClusters;
    }

    private static double[] computeDistance(ArrayList<Vector> centers, Vector point) {
        double[] distances = new double[centers.size()];
        for (int i = 0; i < centers.size(); i++) {
            //Spark sqdist.
            //double d = Vectors.sqdist(point, centers.get(i));                                 //16s
            //Euclidean distance with Vector.
            //double d = Distances.distanceEuclidean(point, centers.get(i));                    //25s
            //Euclidean distance with Array.
            double d = Distances.distanceEuclidean(point.toArray(), centers.get(i).toArray());  //16s // DEFAULT
            //double d = new org.apache.commons.math3.ml.distance.EuclideanDistance().compute(point.toArray(), centers.get(i).toArray());

            // Chebyshev.
            //double d = Distances.distanceChebyshev(point.toArray(), centers.get(i).toArray());
            //double d = new org.apache.commons.math3.ml.distance.ChebyshevDistance().compute(point.toArray(), centers.get(i).toArray());

            // Manhattan.
            //double d = Distances.distanceManhattan(point.toArray(), centers.get(i).toArray());
            //double d = new org.apache.commons.math3.ml.distance.ManhattanDistance().compute(point.toArray(), centers.get(i).toArray());

            // Minkowski. 3x slower than Euclidean.
            //double d = Distances.distanceMinkowski(point.toArray(), centers.get(i).toArray());

            // Canberra. Returns 1 testcluster.
            //double d = new org.apache.commons.math3.ml.distance.CanberraDistance().compute(point.toArray(), centers.get(i).toArray());

            // EarthMovers.
            //double d = new org.apache.commons.math3.ml.distance.EarthMoversDistance().compute(point.toArray(), centers.get(i).toArray());

            // Cosine.
            //double d = Distances.distanceCosine(point, centers.get(i));
            //double d = Distances.distanceCosine(point.toArray(), centers.get(i).toArray());

            distances[i] = d;
        }
        return distances;
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

    public static Vector sumArrayByColumn(Vector t1, Vector t2) {
        double[] tab = new double[t1.size()];
        for (int i = 0; i < t1.size(); i++) {
            tab[i] = t1.apply(i) + t2.apply(i);
        }
        return new DenseVector(tab);
    }

    public static Vector sumArrayByColumn(double[] t1, double[] t2) {
        double[] tab = new double[t1.length];
        for (int i = 0; i < t1.length; i++) {
            tab[i] = t1[i] + t2[i];
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

    public static Vector divideArray(double[] t1, Long l) {
        double[] tab = new double[t1.length];
        for (int i = 0; i < t1.length; i++) {
            tab[i] = t1[i] / l;
        }
        return new DenseVector(tab);
    }
}