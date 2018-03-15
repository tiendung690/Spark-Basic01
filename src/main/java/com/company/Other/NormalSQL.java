package com.company.Other;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.rdd.JdbcRDD$;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCPartition;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import org.apache.spark.sql.internal.*;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import scala.collection.Seq;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by as on 02.03.2018.
 */
public class NormalSQL {
    public static void main(String[] args) {

        // INFO DISABLED
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Spark_JDBC")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);

        Dataset<Row> jdbcDF = sparkSession.read()
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/rsds_data")
                .option("dbtable", "slowa_kluczowe")
                .option("user", "root")
                .option("inferSchema", false)
                .option("password", "")
                .format("org.apache.spark.sql.execution.datasources.jdbc.DefaultSource")
                .load()
                .limit(10);

        jdbcDF.show();


        jdbcDF.take(((int) jdbcDF.count()));
        System.out.println("***************************************");
        //Long start = Long.valueOf(6);
        //Long end= Long.valueOf(7);
        AtomicLong atIndex = new AtomicLong(0);

        JavaRDD<Row> filteredRDD = jdbcDF
                .toJavaRDD()
                .zipWithIndex()
               // .filter((Tuple2<Row,Long> v1) -> v1._2 >= start && v1._2 < end)
                .filter((Tuple2<Row,Long> v1) -> v1._2==atIndex.get())
                .map(r -> r._1);

        Dataset<Row> filteredDataFrame = sparkSession.createDataFrame(filteredRDD, jdbcDF.schema());
        filteredDataFrame.show();

        System.out.println("***************************************");

//        DataFrame df;
//        SQLContext sqlContext;
//        Long start;
//        Long end;
//
//        JavaPairRDD<Row, Long> indexedRDD = df.toJavaRDD().zipWithIndex();
//        JavaRDD filteredRDD = indexedRDD.filter((Tuple2<Row,Long> v1) -> v1._2 >= start && v1._2 < end);
//        DataFrame filteredDataFrame = sqlContext.createDataFrame(filteredRDD, df.schema());


        try {
            // create our mysql database connection
            String myDriver = "com.mysql.jdbc.Driver";
            String myUrl = "jdbc:mysql://localhost:3306/rsds_data";
            Class.forName(myDriver);
            Connection conn = DriverManager.getConnection(myUrl, "root", "");

            // our SQL SELECT query.
            // if you only need a few columns, specify them by name instead of using "*"
            String query = "SELECT * FROM slowa_kluczowe";

            // create the java statement
            Statement st = conn.createStatement();

            // execute the query, and get a java resultset
            ResultSet rs = st.executeQuery(query);

            ResultSetMetaData rsmd = rs.getMetaData();
            System.out.println("columns: " + rsmd.getColumnCount());
            System.out.println("Column Name of 1st column: " + rsmd.getColumnName(1) + "--" + rsmd.getColumnTypeName(1));
            System.out.println("Column Name of 1st column: " + rsmd.getColumnName(2) + "--" + rsmd.getColumnTypeName(2));


//            CatalystSqlParser c = new CatalystSqlParser(new SQLConf());
//
//            StructField[] structFields = new StructField[]{
//                    new StructField(rsmd.getColumnName(1), c.parseDataType(rsmd.getColumnTypeName(1)), true, Metadata.empty()),
//                    new StructField(rsmd.getColumnName(2), c.parseDataType(rsmd.getColumnTypeName(2)), true, Metadata.empty())
//            };
//            StructType structType = new StructType(structFields);


            List<Row> rows = new ArrayList<>();

             //iterate through the java resultset
//            while (rs.next()) {
//                Row row = RowFactory.create(JdbcRDD.resultSetToObjectArray(rs));
//                rows.add(row);
//            }

            rs.next();
            Row row = RowFactory.create(JdbcRDD.resultSetToObjectArray(rs));
            rows.add(row);


            Dataset<Row> df = sparkSession.createDataFrame(rows, jdbcDF.schema());
            df.printSchema();
            df.show();
            System.out.println(df.count());



            // typy danych kolumn
            for (int i = 0; i < df.columns().length; i++) {
                System.out.println(df.schema().fields()[i].dataType().catalogString());/// np. StringType -> string
            }

            st.close();

            ///  MODYFIKACJA WIERSZA
            Row r = df.first();
            System.out.println("--------------MODYFIKACJA WIERSZA");
            //System.out.println("wartosci: "+r.get(0)+" , "+r.get(1));


            // 1 mozliwosc wykonania edycji
            Object[] obj =new Object[r.size()];
            for (int i = 0; i < r.size(); i++) {
                System.out.println("XX "+r.get(i));
                if(i==1){
                    obj[i]="nowaWARTOSC";
                }else{
                    obj[i]=r.get(i);
                }
            }

            // 2 mozliwosc wykonania edycji
            Object[] obj2 = (Object[]) df.first().toSeq().toArray( scala.reflect.ClassTag$.MODULE$.apply(Object.class));
            obj2[1]="kupa";


            Row r2 = RowFactory.create(obj2);
            List<Row> rows2 = new ArrayList<>();
            rows2.add(r2);
            Dataset<Row> df2 = sparkSession.createDataFrame(rows2, jdbcDF.schema());
            df2.show();
            df2.printSchema();

            /// ROBIE NOWY DATASET Z POPRZEDNIEGO Z struct i row

            List<Row> rows3 = new ArrayList<>();
            rows3.add(df2.first());

            Dataset<Row> df3 = sparkSession.createDataFrame(rows3, df2.schema());

            System.out.println("ROW-STRUCTTYPE");
            df3.show();
            df3.printSchema();
            System.out.println(df2.schema().fieldNames().length);
            System.out.println(rows3.get(0).size());
            System.out.println(df2.schema().toString());
            System.out.println(Arrays.toString(df2.schema().fieldNames()));
            System.out.println(df2.schema().fields()[1].dataType());

        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }
    }
}

//    Object get(int i)
//    Returns the value at position i. If the value is null, null is returned. The following is a mapping between Spark SQL types and return types:
//
//        BooleanType -> java.lang.Boolean
//        ByteType -> java.lang.Byte
//        ShortType -> java.lang.Short
//        IntegerType -> java.lang.Integer
//        FloatType -> java.lang.Float
//        DoubleType -> java.lang.Double
//        StringType -> String
//        DecimalType -> java.math.BigDecimal
//
//        DateType -> java.sql.Date
//        TimestampType -> java.sql.Timestamp
//
//        BinaryType -> byte array
//        ArrayType -> scala.collection.Seq (use getList for java.util.List)
//        MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
//        StructType -> org.apache.spark.sql.Row
