import ch.lambdaj.Lambda;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.knowm.xchart.QuickChart;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import scala.Tuple2;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class VideoGamesAndCrimes  extends SparkInitializer{

    public static void main(String[] args) {
        initialize();

        JavaRDD<String> crimesLines =
                SparkInitializer.sparkContext.textFile("dataset\\VideoGamesAndSuicide\\CrimesFrom2010_TEST.csv");
        JavaRDD<String> videoGamesLines =
                SparkInitializer.sparkContext.textFile("dataset\\VideoGamesAndSuicide\\VideoGames.csv");
        crimesLines.cache();
        videoGamesLines.cache();

        String crimesHeader = crimesLines.first();
        String videoGamesHeader = videoGamesLines.first();

        JavaRDD<String> crimesData = crimesLines.filter(line -> !line.equals(crimesHeader));
        JavaRDD<String> videoGamesData = videoGamesLines.filter(line -> !line.equals(videoGamesHeader));

        JavaRDD<String> actionGamesData =
                videoGamesData.filter(line -> line.split(",")[4].equalsIgnoreCase("Action") && line.split(",")[3].matches("([2][0][1][0-6]+)"));

        JavaPairRDD<String, Double> actionGamesPerYear =
                actionGamesData.mapToPair(row -> new Tuple2<>(row.split(",")[3], Double.parseDouble(row.split(",")[6])));

        //Grouped all records to be a <K> of years, and list of all action games sales
        JavaPairRDD<String, Iterable<Double>> groupedActionGamesPerYear =
                actionGamesPerYear.groupByKey();

        //Map of years<K>, summation of sales in NA<V>
        Map<String, Iterable<Double>> mapActionGamesPerYear = groupedActionGamesPerYear.collectAsMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
        System.out.println("Print all video games sales per year");
        mapActionGamesPerYear.forEach((s, integers) -> System.out.println(s + ": " + Lambda.sum(integers)));

        JavaRDD<String> crimesDataYears =
                crimesData.map(row -> row.split(",")[2].split("/")[2]);

        JavaPairRDD<String, Iterable<String>> crimesDataYearsCount =
                crimesDataYears.groupBy(crime->crime);

        //Map of years<K>, summation of sales in NA<V>
        Map<String, Iterable<String>> mapCrimesCount = crimesDataYearsCount.collectAsMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
        System.out.println("Print all crimes per year");
        mapCrimesCount.forEach((s, integers) -> Lambda.count(integers).forEach((o, integer) -> System.out.println(integer)));
    }


}
