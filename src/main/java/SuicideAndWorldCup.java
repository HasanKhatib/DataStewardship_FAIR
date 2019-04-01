import ch.lambdaj.Lambda;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.knowm.xchart.QuickChart;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import scala.Tuple2;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SuicideAndWorldCup extends SparkInitializer{

    public static void main(String[] args) {
        if (false) {
            double[] xData = new double[]{0.0, 1.0, 2.0};
            double[] yData = new double[]{2.0, 1.0, 0.0};

            // Create Chart
            XYChart chart =
                    QuickChart.getChart("Sample Chart",
                            "X", "Y", "y(x)", xData, yData);

            // Show it
            new SwingWrapper(chart).displayChart();
        }
        initialize();

        JavaRDD<String> lines = SparkInitializer.sparkContext.textFile("dataset\\SuicideRates1985To2016.csv");
        lines.cache();

        String first = lines.first();
        JavaRDD<String> dataLines = lines.filter(line -> !line.equals(first));

        JavaRDD<String[]> lineTokens = dataLines.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                return s.split(",");
            }
        });

        JavaRDD<String[]> filteredRDD = lineTokens.filter(line -> line[0].equalsIgnoreCase(SparkInitializer.country));
        filteredRDD.foreach(new VoidFunction<String[]>() {
            @Override
            public void call(String[] strings) throws Exception {
                System.out.println(strings[0]);
            }
        });

        JavaPairRDD<String, Integer> yearWithSuicideNumber =
                filteredRDD.mapToPair(row -> new Tuple2<>(row[1], Integer.parseInt(row[4])));
        JavaPairRDD<String, Iterable<Integer>> groupedYearWithNumber =
                yearWithSuicideNumber.groupByKey();
        Map<String, Iterable<Integer>> mapResultYearWithNumbers = groupedYearWithNumber.collectAsMap();
        mapResultYearWithNumbers.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new))
                .forEach((s, integers) -> System.out.println(s + ": " + Lambda.sum(integers)));
    }


}
