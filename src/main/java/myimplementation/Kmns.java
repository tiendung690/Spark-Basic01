package myimplementation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
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
public class Kmns {

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

//                DenseVector dd = v1._2().toDense();//.copy();
//                BLAS$.MODULE$.axpy(1.0, v2._2(), dd);
//                return new Tuple2<>(v1._1() + v2._1(), dd);


                return new Tuple2<>(v1._1() + v2._1(), Util.sumArrayByColumn(v1._2(), v2._2()));

                //  return new Tuple2<>(v1._1() + v2._1(), new FastAxpy().axpy(1.0, v2._2(), v1._2().toDense()));
            });

            // 4
            JavaPairRDD<Integer, Vector> s4 = s3.mapValues(v1 -> {
//                Vector v = v1._2();
//                BLAS$.MODULE$.scal(1.0 / v1._1(), v);
//                return v;

                Vector v = Util.divideArray(v1._2(), v1._1());
                return v;
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
            //double d = Distances.distanceEuclidean2(point, centers.get(i)); // 7s !!!!!!!!!!!!!! BAD BAD BAD BAD
            //double d = Vectors.sqdist(point, centers.get(i)); //3,7s - 4,2s


            double d = Distances.distanceEuclidean(point.toArray(), centers.get(i).toArray()); //4s -3,9 OK OK OK OK OK OK OK

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