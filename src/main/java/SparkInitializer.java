import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

abstract class SparkInitializer {
    public static SparkConf conf = null;
    public static JavaSparkContext sparkContext = null;

    public static void initialize() {
        conf = new SparkConf();
        conf.setAppName("DStewardship");
        conf.setMaster("local");
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        sparkContext = new JavaSparkContext(conf);
        sparkContext.setLogLevel("ERROR");
    }
}
