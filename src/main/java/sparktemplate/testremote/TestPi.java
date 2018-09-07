package sparktemplate.testremote;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class TestPi {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Spark_Experiment_Pi")
                // Cluster address.
                .setMaster("spark://10.2.28.17:7077")
                // Project jar. Build artifact without Spark libs.
                .setJars(new String[] { "out/artifacts/SparkProject_jar/SparkProject.jar" })
                // Driver address.
                .set("spark.driver.host", "10.2.28.31");

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
