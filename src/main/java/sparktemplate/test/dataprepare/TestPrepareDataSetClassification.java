package sparktemplate.test.dataprepare;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.*;
import scala.collection.immutable.Seq;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClassification;
import sparktemplate.datasets.MemDataSet;
import sparktemplate.strings.ClassificationStrings;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

/**
 * Created by as on 07.08.2018.
 */
public class TestPrepareDataSetClassification {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        //Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("TestPrepareDataSetClassification")
                .setMaster("local[*]");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

        String path = "data_test/kdd_test.csv";
        //String path = "data_test_prepare/data_classification_mixed.csv";
        //String path = "data_test_prepare/data_classification_only_symbolical.csv";
        //String path = "data_test_prepare/data_classification_only_numerical.csv";
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path);

        // Raw data.
        Dataset<Row> ds = memDataSet.getDs();
        ds.printSchema();
        ds.show();

        // Prepared data.
        Dataset<Row> ds2 = DataPrepareClassification.prepareDataSet(ds);
        ds2.show(false);
        ds2.printSchema();


        //// PARQUET
        //// Save prepared data. (Parquet format)
        // ds2.write().parquet("data_test/kdd_test_prepared_parquet");

        //// Load prepared data.
        //Dataset<Row> dsFromJson = new DataFrameReader(sparkSession).parquet("data_test/kdd_train_prepared_parquet");
        //dsFromJson.show();
        //dsFromJson.printSchema();



        //// JSON
        //// To save as JSON, features must be DenseVector.
        //// Convert.
        //Dataset<Row> converted = ds2;//DataPrepare.convertVectorColToDense(ds2, ClassificationStrings.featuresCol);
        //converted.show();
        //converted.printSchema();
        //// Save.
        //converted.write().json("data_test/kdd_test_prepared_json");

        //// provide schema.
        //StructType schema = new StructType(new StructField[]{
        //        new StructField("features", new VectorUDT(), false, Metadata.empty()),
        //        new StructField("label", DataTypes.StringType, true, Metadata.empty())
        //});
        //// Load prepared data.
        //MemDataSet memDataSet2 = new MemDataSet(sparkSession);
        //memDataSet2.loadDataSetJSON("data_test/kdd_test_prepared_json", schema);
        //Dataset<Row> dsFromJson = memDataSet2.getDs();
        //dsFromJson.show();
        //dsFromJson.printSchema();


    }
}
