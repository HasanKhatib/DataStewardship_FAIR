import ch.lambdaj.Lambda;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.spark_project.guava.collect.Iterables;
import scala.Tuple2;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VideoGamesAndCrimes extends SparkInitializer {

    public static void main(String[] args) {
        initialize();

        //sum the sales numbers for each year
        JavaPairRDD<String, Double> groupedActionGamesPerYearAndSum = getGroupedActionGamesPerYear();
        //Map of years<K>, summation of sales in NA<V>
        Map<String, Double> mapActionGamesPerYear = groupedActionGamesPerYearAndSum
                .collectAsMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        //convert the iterable to a summed number
        JavaPairRDD<String, Double> crimesDataYearsCount = getCrimesDataYearsCount();
        //Map of years<K>, count of incedents
        Map<String, Double> mapCrimesCount = crimesDataYearsCount.collectAsMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        DrawChart(mapActionGamesPerYear, mapCrimesCount);
    }

    private static JavaPairRDD<String, Double> getCrimesDataYearsCount() {
        JavaRDD<String> crimesLines =
                SparkInitializer.sparkContext.textFile("C:\\datastewardship_datasets\\LA_Crime_Data_from_2010_to_Present.csv");

        crimesLines.cache();
        String crimesHeader = crimesLines.first();
        JavaRDD<String> crimesData = crimesLines
                .filter(line -> !line.equals(crimesHeader) && !(Integer.parseInt(line.split(",")[2].split("/")[2]) > 2015));

        //get only the year out of crimes data set
        JavaRDD<String> crimesDataYears =
                crimesData.map(row -> row.split(",")[2].split("/")[2]);
        //group the column so we have a year, and a list of same years in iterable
        JavaPairRDD<String, Iterable<String>> crimesDataYearsGrouped =
                crimesDataYears.groupBy(crime -> crime);


        return crimesDataYearsGrouped.mapValues(yearsList -> (double) Iterables.size(yearsList));
    }

    private static JavaPairRDD<String, Double> getGroupedActionGamesPerYear() {
        JavaRDD<String> videoGamesLines =
                SparkInitializer.sparkContext.textFile("C:\\datastewardship_datasets\\Video_Games_Sales.csv");
        videoGamesLines.cache();
        String videoGamesHeader = videoGamesLines.first();
        JavaRDD<String> videoGamesData = videoGamesLines
                .filter(line -> !line.equals(videoGamesHeader));
        //Take only action games from the games sales dataset
        JavaRDD<String> actionGamesData =
                videoGamesData.filter(line ->
                        line.split(",")[4].equalsIgnoreCase("Action") && line.split(",")[3].matches("([2][0][1][0-5]+)"));
        //create pairs of the year of selling action game and the amount of sales for this action game
        JavaPairRDD<String, Double> actionGamesPerYear =
                actionGamesData.mapToPair(row -> new Tuple2<>(row.split(",")[3], Double.parseDouble(row.split(",")[6])));

        //Grouped all records to be a <K> of the year, and list of all action games sales
        JavaPairRDD<String, Iterable<Double>> groupedActionGamesPerYear =
                actionGamesPerYear.groupByKey();
        return groupedActionGamesPerYear.mapValues(numbers -> Lambda.sum(numbers).doubleValue());

    }

    //1st Action Games
    //2nd Crimes
    public static void DrawChart(Map<String, Double> firstDS, Map<String, Double> secondDS) {
        System.out.println("Action Games DS");
        firstDS.forEach((k, v) -> System.out.println(k + ": " + v));
        System.out.println("Incedents DS");
        secondDS.forEach((k, v) -> System.out.println(k + ": " + v));

        ArrayList<String> firstDSActionGamesYears = new ArrayList<>(firstDS.keySet());
        ArrayList<Double> firstDSActionGamesCount = new ArrayList<Double>(firstDS.values());

        ArrayList<String> secondDSYears = new ArrayList<>(secondDS.keySet());
        ArrayList<Double> secondDSIncedents = new ArrayList<Double>(secondDS.values());
        IntStream.range(0, secondDSIncedents.size())
                .forEach(i -> secondDSIncedents.set(i, secondDSIncedents.get(i) / 1000));

        // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title("Correlation between Action Games sold in NA and Crimes in LA").xAxisTitle("Years").yAxisTitle("Count Number").theme(Styler.ChartTheme.GGPlot2).build();

        // Series
        chart.addSeries("Action Games in NA (# in Millions)", firstDSActionGamesYears, firstDSActionGamesCount);
        chart.addSeries("Crimes in LA (# in Thousands)", secondDSYears, secondDSIncedents);

        new SwingWrapper<CategoryChart>(chart).displayChart();
    }
}
