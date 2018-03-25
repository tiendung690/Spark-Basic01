import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by as on 14.03.2018.
 */
public class Main {
    public static void main(String[] args) {
        //Logger.getLogger("org").setLevel(Level.OFF);
        //Logger.getLogger("akka").setLevel(Level.OFF);

        //Logger.getLogger("INFO").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Spark_App_Test_0.1")
                //.set("spark.driver.allowMultipleContexts", "true")
                .setMaster("spark://10.2.28.17:7077")
                .set("spark.driver.host","10.2.28.31")
                .set("spark.executor.memory", "4g");
                //.set("spark.execution.cores", "1");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        int NUM_SAMPLES = 100;
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        System.out.println(jsc.parallelize(l).count());

//        long count = jsc.parallelize(l).filter(i -> {
//            double x = Math.random();
//            double y = Math.random();
//            return x*x + y*y < 1;
//        }).count();
//        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);


    }
}
