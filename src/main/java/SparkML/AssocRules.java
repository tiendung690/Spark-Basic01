package SparkML;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.*;
import sparktemplate.dataprepare.DataPrepareAssociations;

/**
 * Created by as on 11.03.2018.
 */
public class AssocRules {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Spark_FP_GROWTH")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);


        String data_path = "data/mllib/koszyk.txt"; //"data/mllib/kdd_10_proc.txt";  //"data/mllib/kdd_10_proc.txt"
        Dataset<Row> df = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(data_path);
//              .limit(5)//;
//              .select("duration", "protocol_type", "service",
//                        "flag", "src_bytes", "dst_bytes", "land",
//                        "wrong_fragment", "urgent", "hot",
//                        "num_failed_logins","logged_in","num_compromised",
//                        //"root_shell","su_attempted","num_root",
//                        "num_file_creations","num_shells", "class");

        df.printSchema();
        System.out.println(df.count());
        //df.randomSplit(new double[]{0.2,0.8})[0].limit(1000);

        Dataset<Row> df2 = DataPrepareAssociations.prepareDataSet(df, sparkSession);

//        // find columns with StringType from dataset
//        List<String> listString = new ArrayList<>();
//
//        for (StructField o : df.schema().fields()) {
//            if (o.dataType().equals(DataTypes.StringType)) {
//                listString.add(o.name());
//            }
//        }
//
//        System.out.println("StringType in Dataset: " + listString.toString());
//        String[] stringArray = listString.toArray(new String[0]);
//
//
//        Dataset<Row> df2 = df.drop(df.drop(stringArray).columns());
//        df2.printSchema();
//        df2.show();
//
//
//        // create new structtype with only String types
//        String[] cols = df2.columns();
//        StructType structType = new StructType();
//        for (String col : cols) {
//            structType = structType.add(col, DataTypes.StringType, false);
//        }
//        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);
//
//        // create new dataset with concat col names + values
//        Dataset<Row> dfX = df2.map(value -> {
//            Object[] obj = new Object[value.size()];
//            for (int i = 0; i < value.size(); i++) {
//                //System.out.println(value.get(i)+cols[i]);
//                obj[i] = value.get(i) + "-" + cols[i];
//            }
//            return RowFactory.create(obj);
//        }, encoder);
//
//        dfX.show();
//        dfX.printSchema();
//
//        // row -> String  ","
//        Dataset<String> ds1 = dfX
//                .map(row -> row.mkString(","), Encoders.STRING());
//
//        // row String to Array
//        JavaRDD<Row> rows = ds1.toJavaRDD().map(v1 -> RowFactory.create(new String[][]{v1.split(",")}));
//
//
//        // new StructType
//        StructType schema2 = new StructType(new StructField[]{
//                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
//        });
//
//        //ExpressionEncoder<Row> encoder2 = RowEncoder.apply(schema2);
//        //Dataset<Row> rows2 = ds1.map(v1 -> RowFactory.create(new String[][]{v1.split(",")}), encoder2);
//
//
//        // create dataset from parts
//        Dataset<Row> input = sparkSession.createDataFrame(rows, schema2);
//
//        input.printSchema();
//        input.show(1, false);


        FPGrowthModel model = new FPGrowth()
                .setItemsCol("text")
                .setMinSupport(0.01)
                .setMinConfidence(0.4)
                .fit(df2);

        // Display frequent itemsets.
        model.freqItemsets().show(false);
        // Display generated association rules.
        Dataset<Row> assocRules = model.associationRules();
        assocRules.show(false);
        assocRules.printSchema();
        // transform examines the input items against all the association rules and summarize the
        // consequents as prediction
        Dataset<Row> assocRules2 = model.transform(df2);
        assocRules2.show(false);
        assocRules2.printSchema();

        //assocRules.toJSON().toDF().write().save("omg");//.saveAsTextFile("lol2");
        //assocRules.rdd().saveAsTextFile("HEHE");

//        assocRules.toDF()
//                .write()
//                .format("org.apache.spark.sql.execution.datasources.json.DefaultSource2")
//                .json("jsonFormat");

        assocRules.write().mode(SaveMode.Overwrite).json("jsonik");

        Dataset<Row> dd = sparkSession.read().json("jsonik");
        dd.show(5);

        // .format("org.apache.spark.sql.execution.datasources.json.DefaultSource")

//        assocRules.write()
//                .format("com.databricks.spark.csv")
//                .option("header", "true")
//                .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
//                .save("fileZIP.csv");
//
//        assocRules.write()
//                .format("com.databricks.spark.csv")
//                .option("header", "true")
//                .save("file.csv");


    }
}
