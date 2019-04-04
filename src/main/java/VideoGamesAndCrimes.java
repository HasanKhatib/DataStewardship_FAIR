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

        JavaRDD<String> crimesLines =
                SparkInitializer.sparkContext.textFile("C:\\dataset\\VideoGamesAndSuicide\\Crime_Data_from_2010_to_Present.csv");
        JavaRDD<String> videoGamesLines =
                SparkInitializer.sparkContext.textFile("C:\\dataset\\VideoGamesAndSuicide\\VideoGames.csv");
        crimesLines.cache();
        videoGamesLines.cache();

        String crimesHeader = crimesLines.first();
        String videoGamesHeader = videoGamesLines.first();

        JavaRDD<String> crimesData = crimesLines.filter(line -> !line.equals(crimesHeader) && !(Integer.parseInt(line.split(",")[2].split("/")[2]) > 2015));
        JavaRDD<String> videoGamesData = videoGamesLines.filter(line -> !line.equals(videoGamesHeader));

        JavaRDD<String> actionGamesData =
                videoGamesData.filter(line -> line.split(",")[4].equalsIgnoreCase("Action") && line.split(",")[3].matches("([2][0][1][0-5]+)"));

        JavaPairRDD<String, Double> actionGamesPerYear =
                actionGamesData.mapToPair(row -> new Tuple2<>(row.split(",")[3], Double.parseDouble(row.split(",")[6])));

        //Grouped all records to be a <K> of years, and list of all action games sales
        JavaPairRDD<String, Iterable<Double>> groupedActionGamesPerYear =
                actionGamesPerYear.groupByKey();

        JavaPairRDD<String, Double> groupedActionGamesPerYearAndSum =
                groupedActionGamesPerYear.mapValues(numbers -> Lambda.sum(numbers).doubleValue());
        //Map of years<K>, summation of sales in NA<V>
        Map<String, Double> mapActionGamesPerYear = groupedActionGamesPerYearAndSum
                .collectAsMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        //get only the year out of crimes data set
        JavaRDD<String> crimesDataYears =
                crimesData.map(row -> row.split(",")[2].split("/")[2]);
        //group the column so we have a year, and a list of same years in iterable
        JavaPairRDD<String, Iterable<String>> crimesDataYearsGrouped =
                crimesDataYears.groupBy(crime -> crime);
        //convert the iterable to a summed number
        JavaPairRDD<String, Double> crimesDataYearsCount =
                crimesDataYearsGrouped.mapValues(yearsList -> (double) Iterables.size(yearsList));

        //Map of years<K>, count of incedents
        Map<String, Double> mapCrimesCount = crimesDataYearsCount.collectAsMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        CategoryChart chart = DrawChart(mapActionGamesPerYear, mapCrimesCount);
        new SwingWrapper<CategoryChart>(chart).displayChart();

    }

    //1st Action Games
    //2nd Crimes
    public static CategoryChart DrawChart(Map<String, Double> firstDS, Map<String, Double> secondDS) {
        System.out.println("Action Games DS");
        firstDS.forEach((k, v) -> System.out.println(k + ": " + v));
        System.out.println("Incedents DS");
        secondDS.forEach((k, v) -> System.out.println(k + ": " + v));

        ArrayList<String> firstDSActionGamesYears = new ArrayList<>(firstDS.keySet());
        ArrayList<Double> firstDSActionGamesCount = new ArrayList<Double>(firstDS.values());

        ArrayList<String> secondDSYears = new ArrayList<>(secondDS.keySet());
        ArrayList<Double> secondDSIncedents = new ArrayList<Double>(secondDS.values());
        IntStream.range(0,secondDSIncedents.size())
                .forEach(i->secondDSIncedents.set(i,secondDSIncedents.get(i)/1000));

        // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title("Correlation between Action Games sold in NA and Crimes in LA").xAxisTitle("Years").yAxisTitle("Count Number").theme(Styler.ChartTheme.GGPlot2).build();

        // Series
        chart.addSeries("Action Games in NA (# in Millions)", firstDSActionGamesYears, firstDSActionGamesCount);
        chart.addSeries("Crimes in LA (# in Thousands)", secondDSYears, secondDSIncedents);

        return chart;
    }
}
