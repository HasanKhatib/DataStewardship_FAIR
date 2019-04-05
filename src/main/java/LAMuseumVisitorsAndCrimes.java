import ch.lambdaj.Lambda;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;
import org.spark_project.guava.collect.Iterables;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LAMuseumVisitorsAndCrimes extends SparkInitializer {
    public static void main(String[] args) {
        initialize();

        JavaPairRDD<String, Double> groupedMuseumVisitorsPerYear = getGroupsMuseumVisitorsPerYear();
        //Sorted Map of years<K>, next to values of summed visitors per this year
        Map<String, Double> mapMusuemVisitors = groupedMuseumVisitorsPerYear
                .collectAsMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));


        JavaPairRDD<String, Double> crimesCountPerYear = getCrimesCountPerYear();
        //Sorted Map of years<K>, count of incedents
        Map<String, Double> mapCrimesCount = crimesCountPerYear.collectAsMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        DrawChart(mapMusuemVisitors, mapCrimesCount);
    }

    /**
     * returns a pairs of year and number of crimes happened in this year
     */
    private static JavaPairRDD<String, Double> getCrimesCountPerYear() {
        JavaRDD<String> crimesLines =
                SparkInitializer.sparkContext.textFile("C:\\dataset\\VideoGamesAndSuicide\\Crime_Data_from_2010_to_Present.csv");

        crimesLines.cache();
        String crimesHeader = crimesLines.first();
        JavaRDD<String> crimesData = crimesLines
                .filter(line -> !line.equals(crimesHeader)
                        && !(Integer.parseInt(line.split(",")[2].split("/")[2]) < 2014)
                        && !(Integer.parseInt(line.split(",")[2].split("/")[2]) > 2018)
                );

        //get only the year out of crimes data set
        JavaRDD<String> crimesDataYears =
                crimesData.map(row -> row.split(",")[2].split("/")[2]);
        //group the column so we have a year, and a list of same years in iterable
        JavaPairRDD<String, Iterable<String>> crimesDataYearsGrouped =
                crimesDataYears.groupBy(crime -> crime);


        return crimesDataYearsGrouped.mapValues(yearsList -> (double) Iterables.size(yearsList));
    }

    /**
     * return pairs of year and number of museums visitors in this year in LA
     */
    private static JavaPairRDD<String, Double> getGroupsMuseumVisitorsPerYear() {
        JavaRDD<String> museumVisitorsLines =
                SparkInitializer.sparkContext.textFile("C:\\dataset\\LA_museum_visitors.csv");
        museumVisitorsLines.cache();
        String museumVisitorsHeader = museumVisitorsLines.first();
        JavaRDD<String> museumVisitorsData = museumVisitorsLines
                .filter(line -> !line.equals(museumVisitorsHeader));

        JavaPairRDD<String, Double> summedPerMonthsMuseumVisitors = museumVisitorsData.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String line) throws Exception {
                String[] lineTokens = line.split(",");
                String key = lineTokens[0].split("-")[0];
                Double value = 0d;
                for (int tokenIndex = 1; tokenIndex < lineTokens.length; tokenIndex++) {
                    if (!lineTokens[tokenIndex].isEmpty())
                        value += Double.parseDouble(lineTokens[tokenIndex]);
                }
                return new Tuple2<>(key, value);
            }
        });

        //Grouped visitors by year
        JavaPairRDD<String, Iterable<Double>> groupedMusuemVisitorsByYear =
                summedPerMonthsMuseumVisitors.groupByKey();

        JavaPairRDD<String, Double> summedPerYearMuseumVisitors = groupedMusuemVisitorsByYear.mapValues(values -> Lambda.sum(values).doubleValue());

        return summedPerYearMuseumVisitors;

    }

    /**
     * 1st Museum Visitors
     * 2nd Crimes
     */
    public static void DrawChart(Map<String, Double> firstDS, Map<String, Double> secondDS) {
        //Just printing it out
        System.out.println("Museum Visitors DS");
        firstDS.forEach((k, v) -> System.out.println(k + ": " + v));
        System.out.println("Incedents DS");
        secondDS.forEach((k, v) -> System.out.println(k + ": " + v));

        ArrayList<String> museumVisitorsYears = new ArrayList<>(firstDS.keySet());
        ArrayList<Double> museumVisitorsSum = new ArrayList<Double>(firstDS.values());
        IntStream.range(0, museumVisitorsSum.size())
                .forEach(i -> museumVisitorsSum.set(i, museumVisitorsSum.get(i) / 1000));

        ArrayList<String> crimesYears = new ArrayList<>(secondDS.keySet());
        ArrayList<Double> crimesIncedentsSum = new ArrayList<Double>(secondDS.values());
        IntStream.range(0, crimesIncedentsSum.size())
                .forEach(i -> crimesIncedentsSum.set(i, crimesIncedentsSum.get(i) / 1000));

        // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600)
                .title("Correlation between Action Games sold in NA and Crimes in LA")
                .xAxisTitle("Years")
                .yAxisTitle("Count Number")
                .theme(Styler.ChartTheme.XChart).build();

        // Series
        chart.addSeries("Museum Visitors in LA", museumVisitorsYears, museumVisitorsSum);
        chart.addSeries("Crimes in LA (# in Thousands)", crimesYears, crimesIncedentsSum);

        new SwingWrapper<CategoryChart>(chart).displayChart();
    }

}
