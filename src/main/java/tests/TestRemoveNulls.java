package tests;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import sparktemplate.dataprepare.DataPrepare;
import sparktemplate.dataprepare.DataPrepareClustering;
import sparktemplate.datasets.MemDataSet;

/**
 * Created by as on 19.03.2018.
 */
public class TestRemoveNulls {
    public static void main(String[] args) {
        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Spark_JDBC2")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.eventLog.dir", "file:///C:/logs")
                .set("spark.eventLog.enabled", "true")
                .setMaster("local[*]");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

        String path = "data_test/iris.csv";//"data/mllib/flights_low.csv"; //"data/mllib/iris.csv";
        System.out.println("// TEST MemDataSet");
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path);
        Dataset<Row> ds = memDataSet.getDs();
        ds.show();
        ds.printSchema();
        ds.cache();
        Dataset<Row> ds2 = DataPrepare.fillMissingValues(ds);
        ds2.show();
        Dataset<Row> ds3 = new DataPrepareClustering().prepareDataSet(ds2,false,false);
        ds3.show();
        ds3.printSchema();


//        // find symbolical and numerical values in dataset
//        List<String> listStr = new ArrayList(); // column name
//        List<Integer> listStrIndex = new ArrayList(); // column index
//        List<String> listNum = new ArrayList(); // column name
//        List<Integer> listNumIndex = new ArrayList(); // column index
//
//        int ii = 0;
//        for (StructField o : ds.schema().fields()) {
//            if (o.dataType().equals(DataTypes.StringType) || o.dataType().equals(DataTypes.DateType)) {
//                listStr.add(o.name());
//                listStrIndex.add(ii);
//                ii++;
//            } else if (o.dataType().equals(DataTypes.IntegerType)
//                    || o.dataType().equals(DataTypes.DoubleType)
//                    || o.dataType().equals(DataTypes.StringType)
//                    || o.dataType().equals(DataTypes.FloatType)
//                    || o.dataType().equals(DataTypes.LongType)
//                    || o.dataType().equals(DataTypes.ShortType)){
//                listNum.add(o.name());
//                listNumIndex.add(ii);
//                ii++;
//            } else {
//                ii++;
//            }
//        }
//
//        // K - column, V - replacement value
//        Map<String, Object> mapReplacementValues = new HashMap<>();
//
//        ///////////////////////  NUMERICAL VALUES
//
//        // count dataset values (accept nulls)
//        long ss = ds.map(value -> 1, Encoders.INT()).reduce((v1, v2) -> v1+v2);
//
//        for (int i = 0; i < listNumIndex.size(); i++) {
//
//            int colId = listNumIndex.get(i);
//
//            Double sum = ds.filter(value -> !value.isNullAt(colId))
//                    .map(value -> Double.parseDouble(value.get(colId).toString()), Encoders.DOUBLE())
//                    .reduce((v1, v2) -> v1 + v2);
//
//            Double avg = sum/ss;
//            mapReplacementValues.put(listNum.get(i), avg);
//        }
//
//        /////////////////////////////   STRINGS
//
//        for (int i = 0; i < listStrIndex.size(); i++) {
//
//            int colId = listStrIndex.get(i);
//
//            Dataset<String> words = ds
//                    .filter(value -> !value.isNullAt(colId))
//                    .flatMap(s -> Arrays.asList(s.get(colId).toString().toLowerCase().split(" ")).iterator(), Encoders.STRING())
//                    //.filter(s -> !s.isEmpty())
//                    .coalesce(1); //one partition (parallelism level)
//
//           // words.show();
//
//            Dataset<Row> t2 = words.groupBy("value")
//                    .count()
//                    .toDF("word", "count");
//
//            t2 = t2.sort(functions.desc("count"));
//
//            String commonValue = (String) t2.first().get(0);
//            mapReplacementValues.put(listStr.get(i), commonValue);
//        }
//
//        System.out.println("Replacement values (column, value) :"+Arrays.asList(mapReplacementValues));
//
//        // Fill missing values
//        Dataset<Row> dsWithoutNulls = ds.na().fill(mapReplacementValues);//.na().fill(mapStr);
//        dsWithoutNulls.show();



}






    // ACUMULATOR
//        LongAccumulator accum = javaSparkContext.sc().longAccumulator();
//        ds.filter(value -> !value.isNullAt(0)).foreach(x -> accum.add(x.getInt(0)));
//        System.out.println(accum.value().intValue());


    //REDUCE
//        Double sum = ds.filter(value -> !value.isNullAt(1))
//                //.map(value -> new Double((Double)value.get(1)), Encoders.DOUBLE())
//                .map(value -> Double.parseDouble(value.get(1).toString()), Encoders.DOUBLE())
//                .reduce((v1, v2) -> v1 + v2);
//
//
//        System.out.println(sum + " -- " + ds2.count());

    //  long sum = ds.filter(value -> !value.isNullAt(0)).count();

//        long sum = ds
//                .map(value -> value.isNullAt(0), Encoders.BOOLEAN())
//                .count();
    // System.out.println(sum + " -- " + ds2.count());

    //long ss = ds.map(value -> 1, Encoders.INT()).reduce((v1, v2) -> v1+v2);
    // System.out.println(ss);


//        Dataset<Row> ds2 = ds.filter(value -> !value.isNullAt(0));
//        //ds2.summary().show();
//
//        ds.groupBy().sum().show();

    //ds2.select("a1").describe().show();

//        for (int i = 0; i < listNumIndex.size(); i++) {
//
//            int colId = listNumIndex.get(i);
//
//            Dataset<String> nums = ds
//                    .filter(value -> !value.isNullAt(colId))
//                    .flatMap(s -> Arrays.asList(s.get(colId).toString().toLowerCase().split(" ")).iterator(), Encoders.STRING())
//                    .filter(s -> !s.isEmpty())
//                    .coalesce(1); //one partition (parallelism level)
//
//            Dataset<Row> t2 = nums.groupBy("value")
//                    .count()
//                    .toDF("word", "count");
//
//            t2 = t2.sort(functions.desc("count"));
//           // t2.show();
//
//            String comm = (String) t2.first().get(0);
//            map.put(listStr.get(i), comm);
//        }


    //  (wartosc, kolumna)
    //ds.na().fill(comm, new String[]{"kolor"}).show();
    //  ds.na().fill(map).na().fill(0).show();
    // null sting to val
    // ds.na().fill("sasas").show();
    // null num to val
    // ds.na().fill(0).show();

}


//    public static Dataset<Row> fillMissingValues(Dataset<Row> ds) {
//
//        // find symbolical and numerical
//        List<String> listStr = new ArrayList();
//        List<Integer> listStrIndex = new ArrayList();
//        List<String> listNum = new ArrayList();
//
//        int ii = 0;
//        for (StructField o : ds.schema().fields()) {
//            if (o.dataType().equals(DataTypes.StringType)) {
//                listStr.add(o.name());
//                listStrIndex.add(ii);
//                ii++;
//            } else {
//                listNum.add(o.name());
//                ii++;
//            }
//        }
//
//
//        Map<String, Object> map = new HashMap<>();
//
//        for (int i = 0; i < listStrIndex.size(); i++) {
//
//            int colId = listStrIndex.get(i);
//
//            Dataset<String> words = ds
//                    .filter(value -> !value.isNullAt(colId))
//                    .flatMap(s -> Arrays.asList(s.get(colId).toString().toLowerCase().split(" ")).iterator(), Encoders.STRING())
//                    .filter(s -> !s.isEmpty())
//                    .coalesce(1); //one partition (parallelism level)
//
//            Dataset<Row> t2 = words.groupBy("value") //<k, iter(V)>
//                    .count()
//                    .toDF("word", "count");
//
//            t2 = t2.sort(functions.desc("count"));
//            t2.show();
//
//            String comm = (String) t2.first().get(0);
//            map.put(listStr.get(i), comm);
//        }
//
//
//        //  (wartosc, kolumna)
//        //ds.na().fill(comm, new String[]{"kolor"}).show();
//        Dataset<Row> dsWithoutNulls = ds.na().fill(map).na().fill(0);
//        // null sting to val
//        // ds.na().fill("sasas").show();
//        // null num to val
//        // ds.na().fill(0).show();
//
//        dsWithoutNulls.show();
//        return dsWithoutNulls;
//    }

