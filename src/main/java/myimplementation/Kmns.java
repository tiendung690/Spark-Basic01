package myimplementation;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.transform.FastCosineTransformer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.ann.BreezeUtil;
import org.apache.spark.ml.clustering.ClusteringSummary;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansSummary;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.ml.linalg.*;
import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.mllib.clustering.KMeans$;
import org.apache.spark.mllib.clustering.VectorWithNorm;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.BLAS$;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.mllib.util.MLUtils$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;
import org.apache.spark.storage.StorageLevel;
import org.spark_project.dmg.pmml.Minkowski;
import scala.Tuple2;
import shapeless.Tuple;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;
import spire.macros.ScalaAlgebra;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.LongAccumulator;

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
                .setAppName("KMeans_Implementation_kddcup_10_k100_iter10")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                .set("spark.driver.memory", "2g")
                .set("spark.executor.memory", "2g")
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
        //JavaSparkContext jsc = new JavaSparkContext(sc);

        System.out.println("**********" + sc.defaultParallelism() + "  ," + sc.defaultMinPartitions());

        //String path = "hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt.gz";
        //String path = "hdfs://192.168.100.4:9000/spark/kdd_10_proc.txt.gz";
        //String path = "data/mllib/kdd_10_proc.txt";
        //String path = "data/mllib/kdd_5_proc.txt";
        //String path = "data/mllib/kdd_3_proc.txt";
        //String path = "data/mllib/flights_low.csv";
        //String path = "data/mllib/kddFIX.txt";
        //String path = "data/mllib/kddcup_train.txt";
        //String path = "data/mllib/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup.txt";
        //String path = "hdfs://10.2.28.17:9000/spark/kddcup_train.txt.gz";
        //String path = "hdfs://10.2.28.17:9000/spark/kmean.txt";
        //String path = "data/mllib/kmean.txt";
        //String path = "data/mllib/iris2.csv";
        String path = "data/mllib/creditcard.csv";
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
        JavaPairRDD<Integer, Vector> x3 = convertToRDDModel2(ds);
        if (x3.getStorageLevel() == StorageLevel.NONE()) {
            System.out.println("NONE :::::::" + x3.getStorageLevel().toString());
        } else {
            System.out.println("NOT NONE");
        }
        System.out.println("XXXX12312312323:  " + x3.getStorageLevel().toString());
        x3.persist(StorageLevel.MEMORY_ONLY());
        System.out.println("XXXX12312312323:  " + x3.getStorageLevel().toString());

        int k = 100;
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
        JavaPairRDD<Integer, Vector> x5 = predictAll2(x3, clusterCenters2);

        Dataset<Row> dm = createDataSet2(x5, spark, "features", "prediction");
        dm.cache();
        //dm.show();
        //dm.printSchema();

        printCenters(clusterCenters2);

        ClusteringEvaluator clusteringEvaluator = new ClusteringEvaluator();
        clusteringEvaluator.setFeaturesCol("features");
        clusteringEvaluator.setPredictionCol("prediction");
        System.out.println("EVAL: " + clusteringEvaluator.evaluate(dm));

        //ClusteringSummary clusteringSummary = new ClusteringSummary(dm, "prediction", "features", k);
        //System.out.println(Arrays.toString(clusteringSummary.clusterSizes()));
        dm.unpersist();

        //saveAsCSV(dm);
        //new Scanner(System.in).nextLine();
        spark.close();
    }

    public static ArrayList<Vector> computeCenters(JavaPairRDD<Integer, Vector> x33, ArrayList<Vector> cc) {

        JavaSparkContext jsc = new JavaSparkContext(x33.context());
        org.apache.spark.util.LongAccumulator accum = jsc.sc().longAccumulator("predictAccum");
//        long startTime = System.currentTimeMillis();
//        long endTime = System.currentTimeMillis();
//        accum.add(endTime-startTime);
//        System.out.println("ACCUMULATOR: "+accum.value());


        //x33.cache();
        //x33.persist(StorageLevel.MEMORY_AND_DISK());
        ArrayList<Vector> clusterCenters = new ArrayList<>(cc);
        //System.out.println("!!!!!!!!!!!!:  " + cc.size());
        //JavaRDD<DataModel> x3 =x33;
        double epsilon = 1e-4;
        int max_iter = 20;
        boolean bol = true;
        int ii = 0;
        do {
            // Compute distances, Predict Cluste
            //JavaRDD<DataModel> x3 = predictAll(x33, clusterCenters);
            //x3 = computeDistancesAndPredictCluster(x3, clusterCenters);
            //x3.cache();
            //x3.cache();
            //x3.foreach(f -> System.out.println(f.getCluster()+", "+Arrays.toString(f.getInputData().toArray())));
            ArrayList<Vector> clusterCenters2 = new ArrayList<>(clusterCenters);
            //System.out.println("#########:  " + clusterCenters.size() + ",    2: " + clusterCenters2.size());
            Broadcast<ArrayList<Vector>> ccc = jsc.broadcast(clusterCenters2);
            //final Vector defVec = org.apache.spark.ml.linalg.Vectors.zeros(clusterCenters2.get(0).size());
            //final Long defCount = 0L;
            //Map<Integer, Tuple2<Long, Vector>> xc = x3

            // 1
            JavaPairRDD<Integer, Vector> s1 = predictAll2(x33, clusterCenters2);
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
//                final Vector vv = v1._2().toDense();
//                BLAS.axpy(1.0, v2._2().toDense(), vv);
//                return new Tuple2<>(v1._1() + v2._1(), vv);
                return new Tuple2<>(v1._1() + v2._1(), sumArrayByColumn(v1._2(), v2._2()));
            });
            // 4
            JavaPairRDD<Integer, Vector> s4 = s3.mapValues(v1 -> {
//                Vector v = v1._2();
//                BLAS.scal(1.0 / v1._1(), v);
//                return v;
                Vector v = divideArray(v1._2(), v1._1());
                return v;
            });
            // 5
            long startTime = System.currentTimeMillis();
            Map<Integer, Vector> xc = s4.collectAsMap();
            ////////////////////////////////////////
            long endTime = System.currentTimeMillis();
            accum.add(endTime - startTime);
            ///////////////////////////////////////

//            Map<Integer, Vector> xc = predictAll2(x33, clusterCenters2)
////                    .map(v1 -> {
////
////                        double[] dd = new double[ccc.value().size()];
////                        for (int i = 0; i < ccc.value().size(); i++) {
////                            double d = distanceEuclidean(v1.getInputData().toArray(), cc.get(i).toArray());
////                            dd[i] = d;
////                        }
////                        int index = findLowerValIndex(dd);
////                        DataModel dataModel = new DataModel();
////                        dataModel.setCluster(index);
////                        dataModel.setInputData(v1.getInputData());
////                        return dataModel;
////                    })
////                    .mapPartitions(vvv -> {
////
////                        List<DataModel> list = new ArrayList<>();
////                        while (vvv.hasNext()) {
////                            DataModel v1 = vvv.next();
////                            //System.out.println(Arrays.toString(v1.getInputData().toArray()));
////                            double[] dd = new double[ccc.value().size()];
////                            for (int i = 0; i < ccc.value().size(); i++) {
////                                //double d = distanceEuclidean(v1.getInputData().toArray(), ccc.value().get(i).toArray());
////                                double d = KMeans$.MODULE$.fastSquaredDistance(new VectorWithNorm(Vectors.fromML(v1.getInputData()))
////                                        , new VectorWithNorm(Vectors.fromML(ccc.value().get(i))));
////                                dd[i] = d;
////                            }
////                            list.add(v1.setCluster(findLowerValIndex(dd)));
////                        }
////                        return list.iterator();
////                    })
//                    //As a partition of an RDD is stored as a whole on a node, this transformation does not require shuffling
////                    .mapToPair(t -> {
////                           return new Tuple2<>(t.getCluster(), new Tuple2<>(1L, t.getInputData()));
////                    })
//                    .mapPartitionsToPair(t -> {
//
//                        List<Tuple2<Integer, Tuple2<Long, Vector>>> list = new ArrayList<>();
//                        while (t.hasNext()) {
//                            //DataModel element = t.next();
//                            Tuple2<Integer, Vector> element = t.next();
//                            //System.out.println(element.getCluster()+"__"+Arrays.toString(element.getInputData().toArray()));
//                            //list.add(new Tuple2<>(element.getCluster(), new Tuple2<>(1L, element.getInputData())));
//                            list.add(new Tuple2<>(element._1(), new Tuple2<>(1L, element._2())));
//                        }
//
//                        return list.iterator();
//                    })
//                    .reduceByKey((v1, v2) -> {  /// BLAS ROBI PROBLEMY!!!!!!!!!!!!!!
//
//
//
//                        final Vector vv = v1._2().toDense();
//                        BLAS.axpy(1.0, v2._2().toDense(), vv);
//
//                        return new Tuple2<>(v1._1() + v2._1(), vv);
//                        // return new Tuple2<>(v1._1() + v2._1(), sumArrayByColumn(v1._2(), v2._2()));
//                    }) // SparkContext.getOrCreate().defaultParallelism()*2
//                    .mapValues(v1 -> {
//
//                        Vector v = v1._2();
//                        BLAS.scal(1.0 / v1._1(), v);
//                        return v;
////                        Vector v = divideArray(v1._2(), v1._1());
////                        return v;
//                    })
//                    .collectAsMap();


            ccc.destroy(false);


            double centers_distance = 0.0;
            // ok
            for (int i = 0; i < clusterCenters.size(); i++) {

                Vector tp = xc.get(i);
                if (tp != null) {
                    clusterCenters2.set(i, tp);
                } else {
                    //System.out.println("elseeeee");
                    clusterCenters2.set(i, clusterCenters2.get(i));
                }
                centers_distance += org.apache.spark.ml.linalg.Vectors.sqdist(clusterCenters.get(i), clusterCenters2.get(i));//distanceEuclidean2(clusterCenters.get(i), clusterCenters2.get(i));
//                centers_distance += KMeans$.MODULE$.fastSquaredDistance(new VectorWithNorm(Vectors.fromML(clusterCenters.get(i)), 2.0)
//                        , new VectorWithNorm(Vectors.fromML(clusterCenters2.get(i)), 2.0));
            }

            centers_distance = centers_distance / clusterCenters.size();
            //System.out.println("CENTERS_DISTANCE: " + centers_distance);
            // ok
            boolean coverage = false;
            //clusterCenters2.equals(clusterCenters);
            //centers_distance<epsilon
            if (centers_distance < epsilon || ii == max_iter - 1) {
                //System.out.println("END" + Arrays.toString(clusterCenters2.toArray()));
                bol = false;
            } else {
                //System.out.println("$$$$$$:  " + clusterCenters.size() + ",    2: " + clusterCenters2.size());
                clusterCenters = new ArrayList<>(clusterCenters2);
                ii++;
                System.out.println("ITERATION: " + ii);//+ " " + Arrays.toString(clusterCenters2.toArray()));
            }
            //x33.unpersist();
            System.out.println("ACCUMULATOR: " + accum.value());
        } while (bol);

        return clusterCenters;
    }

    public static JavaRDD<DataModel> convertToRDDModel(Dataset<Row> ds) {
        // Convert dataset to JavaRDD of DataModel
        JavaRDD<DataModel> x3 = ds.toJavaRDD().map(row -> {
            DataModel dataModel = new DataModel();
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

//    public static JavaRDD<DataModel> predictAll(JavaRDD<DataModel> x, ArrayList<Vector> cc) {
//
//        JavaSparkContext jsc = new JavaSparkContext(x.context());
//        Broadcast<ArrayList<Vector>> ccc = jsc.broadcast(cc);
//
//        JavaRDD<DataModel> e = x
//                .mapPartitions(vvv -> {
//
//                    List<DataModel> list = new ArrayList<>();
//                    while (vvv.hasNext()) {
//                        DataModel v1 = vvv.next();
//                        //System.out.println(Arrays.toString(v1.getInputData().toArray()));
//                        double[] dd = new double[ccc.value().size()];
//                        for (int i = 0; i < ccc.value().size(); i++) {
//                            //double d = distanceEuclidean(v1.getInputData().toArray(), ccc.value().get(i).toArray());
//                            double d = KMeans$.MODULE$.fastSquaredDistance(new VectorWithNorm(Vectors.fromML(v1.getInputData()))
//                                    , new VectorWithNorm(Vectors.fromML(cc.get(i))));
//                            dd[i] = d;
//                        }
//
//                        // long startTime = System.nanoTime();
//
//                        int index = findLowerValIndex(dd);
//
////                        long endTime   = System.nanoTime();
////                        long totalTime = endTime - startTime;
////                        System.out.println("******************"+totalTime);
//
//
////                        DataModel dataModel = new DataModel();
////                        dataModel.setCluster(index);
////                        dataModel.setInputData(v1.getInputData());
////                        list.add(dataModel);
//                        list.add(v1.setCluster(index));
//                    }
//                    return list.iterator();
//                });
////                .map(v1 -> {
////
////                    double[] dd = new double[cc.size()];
////                    for (int i = 0; i < cc.size(); i++) {
////                        double d = distanceEuclidean(v1.getInputData().toArray(), cc.get(i).toArray());
////                        //double d = KMeans$.MODULE$.fastSquaredDistance(v1,cc.get(i));
////                        dd[i] = d;
////                    }
////
////                    int index = findLowerValIndex(dd);
////
////                    DataModel dataModel = new DataModel();
////                    dataModel.setCluster(index);
////                    dataModel.setInputData(v1.getInputData());
////                    //dataModel.setDistances(v1.getDistances());
////                    //dataModel.setInputData(v1.vector().asML());
////                    //System.out.println("in: "+Arrays.toString(dataModel.getInputData().toArray())+", c:"+dataModel.getCluster());
////                    return dataModel;
////                });
//        //System.out.println("-------------");
//        return e;
//    }

    public static JavaPairRDD<Integer, Vector> predictAll2(JavaPairRDD<Integer, Vector> x, ArrayList<Vector> cc) {

        JavaSparkContext jsc = new JavaSparkContext(x.context());

        ArrayList<VectorWithNorm> ww = new ArrayList<>();
        for(Vector v : cc){
            ww.add(new VectorWithNorm(Vectors.fromML(v)));
        }

        Broadcast<ArrayList<VectorWithNorm>> ccc = jsc.broadcast(ww);


        JavaPairRDD<Integer, Vector> e = x
                .mapPartitionsToPair(vvv -> {
                    List<Tuple2<Integer, Vector>> list = new ArrayList<>();
                    while (vvv.hasNext()) {
                        Tuple2<Integer, Vector> v1 = vvv.next();
                        double[] dd = computeDistance(ccc.value(), new VectorWithNorm(Vectors.fromML(v1._2())));
                        int index = findLowerValIndex(dd);
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
            //double d = org.apache.spark.ml.linalg.Vectors.sqdist(v1._2(),ccc.value().get(i));
            //double d = distanceEuclidean(v1._2().toArray(), ccc.value().get(i).toArray());
            double d = MLUtils$.MODULE$.fastSquaredDistance(
                    point.vector(), point.norm(),
                    centers.get(i).vector(), centers.get(i).norm(),
                    1e-6);
            //double d = distanceEuclidean2(point, centers.get(i));
            dd[i] = d;
        }
        return dd;
    }

    static double cosineDistance(Vector v1, Vector v2){
       return 1 - BLAS.dot(v1, v2) / org.apache.spark.ml.linalg.Vectors.norm(v1,2.0) / org.apache.spark.ml.linalg.Vectors.norm(v2,2.0);
    }
    static double squaredDistance(Vector a, Vector b) {
        double distance = 0.0;
        int size = a.size();
        for (int i = 0; i < size; i++) {
            double diff = a.apply(i) - b.apply(i);
            distance += diff * diff;
        }
        return distance;
    }

    public static double distanceEuclidean(double[] t1, double[] t2) {

        double sum = 0;
        for (int i = 0; i < t1.length; i++) {
            sum += Math.pow((t1[i] - t2[i]), 2.0);
        }
        return Math.sqrt(sum);
    }

    public static double distanceEuclideanSquaredFast(Vector t1, Vector t2) {
        //System.out.println("########: "+t1.size()+",  "+t2.size());
        //return org.apache.spark.ml.linalg.Vectors.sqdist(t1,t2);
        double sum = 0;
        for (int i = 0; i < t1.size(); i++) {
            sum += Math.pow((t1.apply(i) - t2.apply(i)), 2.0);
        }
        return sum;
    }

    public static double distanceEuclidean2(Vector t1, Vector t2) {
        //System.out.println("########: "+t1.size()+",  "+t2.size());
        //return org.apache.spark.ml.linalg.Vectors.sqdist(t1,t2);
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