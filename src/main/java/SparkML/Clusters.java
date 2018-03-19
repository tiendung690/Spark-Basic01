package SparkML;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import sparktemplate.clustering.DataPrepare;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by as on 12.03.2018.
 */
public class Clusters {
    public static void main(String[] args) {
//        Logger.getLogger("org").setLevel(Level.OFF);
//        Logger.getLogger("akka").setLevel(Level.OFF);
//
//        Logger.getLogger("INFO").setLevel(Level.OFF);
//
//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_FP_GROWTH")
//                .set("spark.driver.allowMultipleContexts", "true")
//                .setMaster("local");
//        SparkContext context = new SparkContext(conf);
//        SparkSession sparkSession = new SparkSession(context);
//
//
//        String data_path = "data/mllib/iris.csv"; //"data/mllib/kddcup_train.txt.gz";   //"data/mllib/kdd_10_proc.txt"
//        Dataset<Row> df = sparkSession.read()
//                .format("com.databricks.spark.csv")
//                .option("header", true)
//                .option("inferSchema", true)
//                .load(data_path);
//                //.limit(1000);
//
//
//
//
//
//
//        // Loads data.
//        Dataset<Row> dataset = DataPrepare.prepareDataset(df);//sparkSession.read().format("libsvm").load("data/mllib/sample_kmeans_data.txt");
//        //dataset.show();
//        System.out.println(Arrays.toString(dataset.schema().fields()));
//
//        // Trains a k-means model.
//        KMeans kmeans = new KMeans().setK(4).setSeed(10L).setFeaturesCol("normFeatures");
//        KMeansModel model = kmeans.fit(dataset);
//
//        // Make predictions
//        Dataset<Row> predictions = model.transform(dataset);
//        predictions.show(10);
//        System.out.println("========== "+predictions.first().get(predictions.columns().length-1)); // SKUPIENIE PIERWSZEGO WIERSZA
//
//        // Evaluate clustering by computing Silhouette score
//        ClusteringEvaluator evaluator = new ClusteringEvaluator();
//
//        double silhouette = evaluator.evaluate(predictions);
//        System.out.println("Silhouette with squared euclidean distance = " + silhouette);
//
//        // Shows the result.
//        Vector[] centers = model.clusterCenters();
//        System.out.println("Cluster Centers: "+centers.length); // LICZBA SKUPIEN
//        for (Vector center : centers) {
//            System.out.println(center);
//        }
//
//
//        predictions.createOrReplaceTempView("clusters");
//        sparkSession.sql("select prediction, count(*) from clusters group by prediction").show();
//
//        predictions.filter(predictions.col("prediction").equalTo(3)).show(30);

    }

//    public static Dataset<Row> prepareDataset(Dataset<Row> df){
//
//        Dataset<Row> prepared;
//
//        // find columns with StringType from dataset
//        List<String> listString = new ArrayList<>();
//        List<String> listOther = new ArrayList<>();
//
//        for (StructField o : df.schema().fields()) {
//            if (o.dataType().equals(DataTypes.StringType)) {
//                listString.add(o.name());
//            }
//            if (!o.dataType().equals(DataTypes.StringType)
//                    && !o.dataType().equals(DataTypes.IntegerType)
//                    && !o.dataType().equals(DataTypes.DoubleType)) {
//                listOther.add(o.name());
//                System.out.println("Other type: " + o.name());
//            }
//        }
//
//        System.out.println("StringType in Dataset: " + listString.toString());
//        System.out.println("Other DataTypes in Dataset (except int,double,string): " + listOther.toString());
//        String[] stringArray = listString.toArray(new String[0]);
//        String[] otherArray = listOther.toArray(new String[0]);
//
//        if (listString.size() > 0) {
//
//            // dataset without columns with StringType
//            Dataset<Row> df2 = df.drop(stringArray);
//            df2.printSchema();
//
//            // dataset with StringType columns
//            Dataset<Row> df3 = df.drop(df2.columns());
//            df3.printSchema();
//
//            // create indexer for each column in dataset
//            PipelineStage[] pipelineStages = new PipelineStage[df3.columns().length];
//
//            for (int i = 0; i < pipelineStages.length; i++) {
//
//                // get current column name
//                String currentCol = df3.columns()[i];
//                // create indexer on column
//                StringIndexer indexer = new StringIndexer()
//                        .setInputCol(currentCol)
//                        .setOutputCol(currentCol + "*");
//                // add indexer to pipeline
//                pipelineStages[i] = indexer;
//            }
//
//            // set stages to pipeline
//            Pipeline pipeline = new Pipeline().setStages(pipelineStages);
//            // fit and transform, drop old columns
//            Dataset<Row> indexed = pipeline.fit(df).transform(df).drop(df3.columns());
//            //indexed.show();
//
//            prepared = indexed.drop(otherArray);
//        } else {
//            System.out.println("No StringType in Dataset");
//            prepared = df.drop(otherArray);
//        }
//
//        StructType schema = new StructType(new StructField[]{
//                // new StructField("label", DataTypes.StringType, false, Metadata.empty()),
//                new StructField("features", new VectorUDT(), false, Metadata.empty())
//        });
//        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
//
//
//        Dataset<Row> vectorsData = prepared.map(s -> {
//
//            double[] doubles = new double[s.size()];
//            for (int i = 0; i < doubles.length; i++) {
//                doubles[i] = Double.parseDouble(String.valueOf(s.get(i)).trim());
//            }
//            return RowFactory.create(Vectors.dense(doubles));
//
//        }, encoder);
//
//        vectorsData.printSchema();
//        //vectorsData.show();
//
//
//        // Normalize each Vector using $L^1$ norm.
//        Normalizer normalizer = new Normalizer()
//                .setInputCol("features")
//                .setOutputCol("normFeatures")
//                .setP(1.0);
//
//        Dataset<Row> l1NormData = normalizer.transform(vectorsData);
//        //l1NormData.show();
//        return l1NormData;
//    }

}


//        System.out.println(listString.toString());
//        String[] stringArray = listString.toArray(new String[0]);
//
//        // dataset without columns with StringType
//        Dataset<Row> df2 =  df.drop(stringArray);
//        df2.printSchema();
//
//        // dataset with StringType columns
//        Dataset<Row> df3 =  df.drop(df2.columns());
//        df3.printSchema();
//
//        // create indexer for each column in dataset
//        PipelineStage[] pipelineStages = new PipelineStage[df3.columns().length];
//
//        for (int i = 0; i < pipelineStages.length; i++) {
//
//            // get current column name
//            String currentCol = df3.columns()[i];
//            // create indexer on column
//            StringIndexer indexer = new StringIndexer()
//                    .setInputCol(currentCol)
//                    .setOutputCol(currentCol+"*");
//            // add indexer to pipeline
//            pipelineStages[i] = indexer;
//        }
//
//        // set stages to pipeline
//        Pipeline pipeline = new Pipeline().setStages(pipelineStages);
//        // fit and transform, drop old columns
//        Dataset<Row> indexed = pipeline.fit(df).transform(df).drop(df3.columns());
//        indexed.show();


//        StructType schema = new StructType(new StructField[]{
//                new StructField("label", DataTypes.StringType, false, Metadata.empty()),
//                new StructField("features", new VectorUDT(), false, Metadata.empty())
//        });
//        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
//
//
//        Dataset<Row> vectorsData = df.map(s -> {
//            return RowFactory.create(s.getString(41),
//                    Vectors.dense(Double.parseDouble(String.valueOf(s.get(30)).trim()),
//                            Double.parseDouble(String.valueOf(s.get(31)).trim().trim()),
//                            Double.parseDouble(String.valueOf(s.get(32)).trim().trim()),
//                            Double.parseDouble(String.valueOf(s.get(33)).trim().trim()),
//                            Double.parseDouble(String.valueOf(s.get(34)).trim().trim()))
//            );
//        }, encoder);