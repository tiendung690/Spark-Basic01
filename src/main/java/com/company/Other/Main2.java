//package com.company;
//
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.ml.Pipeline;
//import org.apache.spark.ml.PipelineModel;
//import org.apache.spark.ml.PipelineStage;
//import org.apache.spark.ml.classification.NaiveBayes;
//import org.apache.spark.ml.classification.NaiveBayesModel;
//import org.apache.spark.ml.feature.StringIndexer;
//import org.apache.spark.ml.linalg.Vector;
//import org.apache.spark.ml.linalg.Vectors;
//import org.apache.spark.mllib.linalg.DenseVector;
//import org.apache.spark.mllib.regression.LabeledPoint;
//import org.apache.spark.mllib.tree.DecisionTree;
//import org.apache.spark.mllib.tree.model.DecisionTreeModel;
//import org.apache.spark.mllib.util.MLUtils;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import scala.Array;
//import scala.Tuple2;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * Created by as on 07.12.2017.
// */
//public class Main2 {
//    public static void main(String[] args) {
//
//        // INFO DISABLED
//        Logger.getLogger("org").setLevel(Level.OFF);
//        Logger.getLogger("akka").setLevel(Level.OFF);
//        Logger.getLogger("INFO").setLevel(Level.OFF);
//
//
//        SparkConf conf = new SparkConf()
//                .setAppName("JavaWordCount")
//                .set("spark.driver.allowMultipleContexts", "true")
//                .setMaster("local");
//        // create Spark Context
//        SparkContext context = new SparkContext(conf);
//        // create spark Session
//        SparkSession sparkSession = new SparkSession(context);
//
//
//
//        /*
//        TAKE HEADERS FROM OTHER FILE
//         */
//        Dataset<Row> headers = sparkSession.read()
//                .format("com.databricks.spark.csv")
//                .option("header", true)
//                .load("data/mllib/kdd_headers.txt");
//
//        String[] h_cols = headers.columns();
//
//
//        Dataset<Row> df = sparkSession.read()
//                .format("com.databricks.spark.csv")
//                .option("header", true) // moze zostac, stare naglowki nadpisane
//                .option("inferSchema", true) // automatically infers column types. It requires one extra pass over the data and is false by default
//                .load("data/mllib/kdd_csv.txt")
//                //.load("C:\\Users\\as\\IdeaProjects\\Converter\\kddcup_train.txt")
//                .toDF(h_cols); // load new headers
////                .load("data/mllib/iris.csv");
//
//
//        //wypisanie z dataseta danych
////        df.foreach(element -> System.out.println(element.getDouble(0)));
////        df.foreach(element -> {
////            String s = element.getString(4);
////            double d;
////            if(s.equals("setosa")){
////                d=0.0;
////            }else if(s.equals("versicolor")){
////                d=1.0;
////            }else if(s.equals("virginica")){
////                d=2.0;
////            }else{
////                d=3.0;
////            }
//////            System.out.println("D "+d);
////        });
//
////        val labeled = tfidf.map(row => LabeledPoint(row.getDouble(0), row(4).asInstanceOf[Vector]))
//
//        // JavaRDD<Row> data = df.toJavaRDD();
//
////        java.util.Vector v = new java.util.Vector(3,3);
//
//        //        Dataset<Row> indexed = indexer.fit(df).transform(df);
//
////        Dataset<Row> indexed2 = new StringIndexer()
////                .setInputCol("t1")
////                .setOutputCol("categoryIndexXD_T1")
////                .fit(df)
////                .transform(df);
//
//        //////////*******************************************************************************************
//
//        StringIndexer indexer = new StringIndexer()
//                .setInputCol("protocol_type")
//                .setOutputCol("protocol_type_index");
//
//        StringIndexer indexer2 = new StringIndexer()
//                .setInputCol("service")
//                .setOutputCol("service_index");
//
//        StringIndexer indexer3 = new StringIndexer()
//                .setInputCol("flag")
//                .setOutputCol("flag_index");
//
//        StringIndexer indexerClass = new StringIndexer()
//                .setInputCol("class")
//                .setOutputCol("class_index");
//
//        Pipeline pipeline = new Pipeline()
//                .setStages(new PipelineStage[]{indexer, indexer2, indexer3, indexerClass});
//
//        // Fit the pipeline to training documents.
//        PipelineModel model = pipeline.fit(df);
//        Dataset<Row> dfA = model.transform(df);//.drop(df.columns()); //df2.drop(df.columns())
//        Dataset<Row> df3 = dfA.drop("protocol_type", "service", "flag", "class");//.drop(df.columns());
//        dfA.withColumnRenamed("class_index", "XDD").printSchema();
//
//        Dataset<Row> dfB = dfA
//                .withColumn("protocol_type", dfA.col("protocol_type_index"))
//                .withColumn("service", dfA.col("service_index"))
//                .withColumn("flag", dfA.col("flag_index"))
//                .withColumn("class", dfA.col("class_index"))
//                .drop("protocol_type_index", "service_index", "flag_index", "class_index");
//
//        dfB.show(2);
//
//
//        System.out.println("--------------------------LABELS-------------------------------");
//
//        dfA.select("protocol_type","protocol_type_index").distinct().orderBy("protocol_type_index").show();
//        dfA.select("service", "service_index").distinct().orderBy("service_index").show();
//        dfA.select("flag", "flag_index").distinct().orderBy("flag_index").show();
//        dfA.select("class", "class_index").distinct().orderBy("class_index").show();
//
//        dfA.select("protocol_type", "protocol_type_index", "service", "service_index", "flag", "flag_index", "class", "class_index").show(10);
//
////        System.out.println("huj: "+Arrays.stream(indexed2.columns()).count());
//
////        final Dataset<Row>[] wtf = new Dataset[]{null};
////        wtf[0].join(df);
////        ///  STREAM ??????????????????????????????? NIE DZIALA
////         Arrays.stream(df.columns())
////                .forEach((s -> {
////
////                    StringIndexer indexerX = new StringIndexer()
////                            .setInputCol(s)
////                            .setOutputCol(s+"_index");
////
////                    wtf[0] = indexerX.fit(df).transform(df);
////
////                    System.out.println(s);
////
////                    wtf[0].show();
////                }));
////
////        Dataset<Row> df2 = wtf[0];
//
////        Arrays.stream(indexed2.columns()).forEach(s -> System.out.println(indexed2.schema().fieldNames()[s]));
//
//        /// ************  dziala **********************************************************************************
//
////        Dataset<Row> df2 = df;
////
////        for (int i = 0; i < df.columns().length; i++) {
////
////            String col = df.columns()[i];
////
////            StringIndexer indexerX = new StringIndexer()
////                    .setInputCol(col)
////                    .setOutputCol(col+"_index");
////
////            df2 = indexerX.fit(df2).transform(df2);
////
//////            System.out.println("column: "+col);
////
//////            wtf.show();
////        }
////
////        System.out.println("original headers:"+Arrays.toString(df.columns()));
////        System.out.println("converted headers:"+Arrays.toString(df2.columns()));
////
////        Dataset<Row> df3 = df2.drop(df.columns()); // USUNIECIE STARYCH KOLUMN  !!!!!!!!!!! //
////
////        System.out.println("1. ALL");
////        df2.show(20,false);
////        System.out.println("2. DROP");
////        df3.show(20,false);
////
////        System.out.println("========== Print Original Data ==============");
////        df.show();
////        System.out.println("/////////////////////////////////////////");
//
//
//        /*
//        CREATE LABELED POINT
//         */
//
//        JavaRDD<Row> data = df3.toJavaRDD();
//        JavaRDD<LabeledPoint> ldat = data
//                .map(row -> {
//                    double d = row.getDouble(row.size() - 1); // pobranie klasy decyzyjnej (ostatnie miejsce)
//
//                    double[] features = new double[row.size() - 1];
//                    for (int i = 0; i < row.size() - 1; i++) {
//                        features[i] = Double.parseDouble(row.get(i).toString());
//                    }
//
////                    row.toSeq().toStream().foreach(s -> System.out.println(s));
//
//
//                    // SPARSE OR DENSE ???
//                    return new LabeledPoint(d, org.apache.spark.mllib.linalg.Vectors.dense(features).toSparse());
//                });
//
//
//     //   ldat.saveAsTextFile("KDDCUP"); // zapis do pliku
//
//        System.out.println("LABELED POINT #  " + ldat.take(2).toString());
//
//
////        JavaRDD<LabeledPoint> ldat = data
////                .map(row -> new LabeledPoint(1.0, org.apache.spark.mllib.linalg.Vectors.dense(row.getDouble(0),row.getDouble(1),row.getDouble(2),row.getDouble(3))));
//
//
////        System.out.println("#  "+ldat.take(5).toString());
////        System.out.println("#  "+ldat.take(10).toString());
//
////        JavaRDD<LabeledPoint> ldat = data
////                .map(row -> new LabeledPoint(row.getDouble(4),
////                        new DenseVector(new double[]{row.getDouble(4),row.getDouble(1),row.getDouble(2),row.getDouble(3)})));
//
//
////        df.rdd.map(r => LabeledPoint(
////                r.getDouble(targetInd), // Get target value
////                // Map feature indices to values
////                Vectors.dense(featInd.map(r.getDouble(_)).toArray)
////        ))
//
//
//        // SHOW CSV
////        System.out.println("========== Print Data ==============");
////        df.show();
////
////
////
////
////
//        System.out.println("/////////////////////////////////////////");
////        JavaSparkContext jsc = new JavaSparkContext(conf);
////        // Load and parse the data file.
////        String datapath = "data/mllib/letterdata_libsvm.data.txt";
////        JavaRDD<LabeledPoint> data2 = MLUtils.loadLibSVMFile(context, datapath).toJavaRDD();
////// Split the data into training and test sets (30% held out for testing)
////        JavaRDD<LabeledPoint>[] splits = ldat.randomSplit(new double[]{0.7, 0.3});
////        JavaRDD<LabeledPoint> trainingData = splits[0];
////        JavaRDD<LabeledPoint> testData = splits[1];
////
////        System.out.println("#  "+data2.take(1).toString());
////// Set parameters.
//////  Empty categoricalFeaturesInfo indicates all features are continuous.
////        int numClasses = 65;
////        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
////        String impurity = "gini";
////        int maxDepth = 5;
////        int maxBins = 32;
////
////// Train a DecisionTree model for classification.
////        DecisionTreeModel model = DecisionTree.trainClassifier(trainingData, numClasses,
////                categoricalFeaturesInfo, impurity, maxDepth, maxBins);
////
////// Evaluate model on test instances and compute test error
////        JavaPairRDD<Double, Double> predictionAndLabel =
////                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
////        double testErr =
////                predictionAndLabel.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testData.count();
////
////        System.out.println("Test Error: " + testErr);
////        System.out.println("Learned classification tree model:\n" + model.toDebugString());
//    }
//}
