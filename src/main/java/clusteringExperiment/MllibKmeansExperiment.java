package clusteringExperiment;

import myimplementation.Util;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.ClusteringSummary;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
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
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by as on 26.04.2018.
 */
public class MllibKmeansExperiment {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Default-Kmeans")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                .set("spark.driver.memory", "2g")
                .set("spark.executor.memory", "2g")
                //.set("spark.driver.memory", "4g")
                //.set("spark.executor.memory", "4g")
                //.set("spark.default.parallelism", "12")
                .setMaster("local[*]");

//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Default_Kmeans")
//                .setMaster("spark://10.2.28.17:7077")
//                .setJars(new String[] { "out/artifacts/SparkProject_jar/SparkProject.jar" })
//                //.set("spark.executor.memory", "15g")
//                .set("spark.default.parallelism", "12")
//                .set("spark.driver.host", "10.2.28.31");

//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Default_Kmeans")
//                .setMaster("spark://192.168.100.4:7077")
//                .setJars(new String[] { "out/artifacts/SparkProject_jar/SparkProject.jar" })
//                //.set("spark.executor.memory", "15g")
//                //.set("spark.default.parallelism", "12")
//                .set("spark.driver.host", "192.168.100.2");

        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);




        //String path = "hdfs://10.2.28.17:9000/spark/kdd_10_proc.txt.gz";
        //String path = "data/mllib/kdd_5_proc.txt";
        //String path = "data/mllib/kmean.txt";
        //String path = "data/mllib/iris2.csv";
        String path = "data/mllib/creditcard.csv";
        //String path = "data/mllib/creditcardBIG.csv";
        //String path = "data/mllib/kddcup_train.txt";
        //String path = "hdfs://192.168.100.4:9000/spark/kdd_10_proc.txt.gz";















        // load mem data
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSet(path);
        //memDataSet.getDs().cache();//.repartition(4);
        DataPrepareClustering dpc = new DataPrepareClustering();
        Dataset<Row> ds1 = dpc.prepareDataSet(memDataSet.getDs(), false, true).select("features");

        //////////////////////////////////////////
        JavaRDD<Row> filteredRDD = ds1
                .toJavaRDD()
                .zipWithIndex()
                // .filter((Tuple2<Row,Long> v1) -> v1._2 >= start && v1._2 < end)
                .filter((Tuple2<Row,Long> v1) ->
                        v1._2==1 || v1._2==2 || v1._2==22 || v1._2==100)
                .map(r -> r._1);


        List<org.apache.spark.ml.linalg.Vector> cx = filteredRDD.map(v -> (org.apache.spark.ml.linalg.Vector)v.get(0)).collect();

        ArrayList<org.apache.spark.ml.linalg.Vector> newCenters = new ArrayList<org.apache.spark.ml.linalg.Vector>();
        newCenters.addAll(cx);

        System.out.println(newCenters.toString());
        ////////////////////////////////////////////////


       // Vector[] vek = new Vector[]{new DenseVector(new double[]{5.1,3.5,1.4,0.2}), new DenseVector(new double[]{5.7,3.8,1.7,0.3})};
        Vector[] vek = new Vector[newCenters.size()];
        for (int i = 0; i <vek.length ; i++) {
            vek[i]=new DenseVector(newCenters.get(i).toArray());
        }


        org.apache.spark.mllib.clustering.KMeans kMeans = new org.apache.spark.mllib.clustering.KMeans()
                .setK(newCenters.size())
                .setEpsilon(1e-4)
                //.setSeed(20L)
                .setMaxIterations(20)
                .setInitialModel(new org.apache.spark.mllib.clustering.KMeansModel(vek));
                //.setInitializationMode(org.apache.spark.mllib.clustering.KMeans.RANDOM());



        JavaRDD<Vector> ok = convertToRDD(ds1);
        System.out.println("FIRST: "+Arrays.toString(ok.first().toArray()));

        org.apache.spark.mllib.clustering.KMeansModel model = kMeans.run(ok.rdd());
       // System.out.println(Arrays.toString(model.clusterCenters()));

        //System.out.println("MAX ITERATIONS "+model.k());



        JavaRDD<Row> ss = ds1.toJavaRDD().map(v1 -> {
            // Transform to mllib.Vector from ml, mllib.Kmeans support only mllib.Vectors
            Vector v = Vectors.fromML((org.apache.spark.ml.linalg.Vector) v1.get(0));
            return RowFactory.create(v1.get(0),model.predict(v));
        });
        StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
                new StructField("prediction", DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> predictions = sparkSession.createDataFrame(ss, schema);

        predictions.printSchema();
        predictions.show();



        ArrayList<Vector> clusterCenters2 = new ArrayList<Vector>(Arrays.asList(model.clusterCenters()));
        Util.printCenters2(clusterCenters2);
//        JavaRDD<Integer> vv = model.predict(convertToRDD(ds1));
//        vv.foreach(vvv-> System.out.println(vv.first()));

         //Make predictions
//        JavaRDD<Integer> prediction = model.predict(convertToRDD(ds1));
      //  Dataset<Row> predictions = RddToDataset(prediction, sparkSession);
        ClusteringEvaluator clusteringEvaluator = new ClusteringEvaluator();
        clusteringEvaluator.setFeaturesCol("features");
        clusteringEvaluator.setPredictionCol("prediction");
        System.out.println("EVAL: "+clusteringEvaluator.evaluate(predictions));

        ClusteringSummary clusteringSummary = new ClusteringSummary(predictions, "prediction", "features", model.k());
        System.out.println(Arrays.toString(clusteringSummary.clusterSizes()));

        //mns.saveAsCSV(kMean.getPredictions());
        //new Scanner(System.in).nextLine();
        sparkSession.close();
    }

    public static JavaRDD<Vector> convertToRDD(Dataset<Row> ds) {
        JavaRDD<org.apache.spark.mllib.linalg.Vector> x3 = ds.toJavaRDD()
                .map(row -> (org.apache.spark.ml.linalg.Vector) row.get(0))
                .map(v1 -> Vectors.fromML(v1));
        return x3;
    }

    public static Dataset<Row> RddToDataset(JavaRDD<Integer> ds, SparkSession s) {

        JavaRDD<Row> ss = ds.map(v1 -> RowFactory.create(v1));

        StructType schema = new StructType(new StructField[]{
                new StructField("prediction", DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> dm = s.createDataFrame(ss, schema);
        return dm;
    }
}
