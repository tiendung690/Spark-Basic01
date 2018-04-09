import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import sparktemplate.datasets.MemDataSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
                .setMaster("spark://192.168.100.4:7077")
                //.set("spark.submit.deployMode", "client")
                //.setJars(new String[]{"out/artifacts/unnamed/unnamed.jar"})
                .set("spark.driver.host","192.168.100.2");
//                .set("spark.executor.memory", "4g");
                //.set("spark.execution.cores", "1");

        SparkContext context = new SparkContext(conf);
        JavaSparkContext jsc = new JavaSparkContext(context);

        int NUM_SAMPLES =1000;
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        long count = jsc.parallelize(l).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x*x + y*y < 1.0;
        }).count();
        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);

        context.stop();



    }
}
