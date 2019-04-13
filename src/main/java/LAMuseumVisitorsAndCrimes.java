import ch.lambdaj.Lambda;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.spark_project.guava.collect.Iterables;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LAMuseumVisitorsAndCrimes extends SparkInitializer {
    public static void main(String[] args) throws Exception {
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
                SparkInitializer.sparkContext.textFile("datastewardship_datasets\\LAexperiment\\LA_Crime_Data_from_2010_to_Present.csv");

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

    /***
     * return pairs of year and number of museums visitors in this year in LA
     * @return
     */
    private static JavaPairRDD<String, Double> getGroupsMuseumVisitorsPerYear() {
        JavaRDD<String> museumVisitorsLines =
                SparkInitializer.sparkContext.textFile("datastewardship_datasets\\LAexperiment\\LA_museum_visitors.csv");
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

    /***
     *
     * @param firstDS
     * @param secondDS
     */
    public static void DrawLines(Map<String, Double> firstDS, Map<String, Double> secondDS) {
        //Just printing it out
        System.out.println("Museum Visitors DS");
        firstDS.forEach((k, v) -> System.out.println(k + ": " + v));
        System.out.println("Incedents DS");
        secondDS.forEach((k, v) -> System.out.println(k + ": " + v));

        double[] museumVisitorsYears = firstDS.keySet().stream().mapToDouble(year -> Double.parseDouble(year)).toArray();
        double[] museumVisitorsSum = firstDS.values().stream().mapToDouble(Double::doubleValue).toArray();

        double[] crimesYears = secondDS.keySet().stream().mapToDouble(year -> Double.parseDouble(year)).toArray();
        double[] crimesIncedentsSum = secondDS.values().stream().mapToDouble(Double::doubleValue).toArray();

        //Export data to csv
        //exportToCSV(museumVisitorsYears, museumVisitorsSum, crimesYears, crimesIncedentsSum);

        //Normalize data to simplify the chart
        IntStream.range(0, museumVisitorsSum.length)
                .forEach(i -> museumVisitorsSum[i] = museumVisitorsSum[i] / 1000);
        IntStream.range(0, crimesIncedentsSum.length)
                .forEach(i -> crimesIncedentsSum[i] = crimesIncedentsSum[i] / 1000);
        // Create Chart
        XYChart chart = new XYChartBuilder().width(800).height(600)
                .title("Correlation between Museum Visitors in LA and Crimes committed in LA")
                .xAxisTitle("Years")
                .yAxisTitle("Count Number")
                .theme(Styler.ChartTheme.XChart).build();

        // Series
        chart.addSeries("Museum Visitors in LA", museumVisitorsYears, museumVisitorsSum);
        chart.addSeries("Crimes in LA (# in Thousands)", crimesYears, crimesIncedentsSum);

        new SwingWrapper<XYChart>(chart).displayChart();
//        new SwingWrapper<CategoryChart>(chart).displayChart();
    }

    /***
     *
     * @param firstDS: Museum Visitors
     * @param secondDS: Crimes
     */
    public static void DrawChart(Map<String, Double> firstDS, Map<String, Double> secondDS) throws Exception {
        //Just printing it out
        System.out.println("Museum Visitors DS");
        firstDS.forEach((k, v) -> System.out.println(k + ": " + v));
        System.out.println("Incedents DS");
        secondDS.forEach((k, v) -> System.out.println(k + ": " + v));

        ArrayList<String> museumVisitorsYears = new ArrayList<>(firstDS.keySet());
        ArrayList<Number> museumVisitorsSum = new ArrayList<Number>(firstDS.values());

        ArrayList<String> crimesYears = new ArrayList<>(secondDS.keySet());
        ArrayList<Number> crimesIncedentsSum = new ArrayList<Number>(secondDS.values());

        //Export data to csv
        exportToCSV(museumVisitorsYears, museumVisitorsSum, crimesYears, crimesIncedentsSum);

        //Normalize data to simplify the chart
        IntStream.range(0, museumVisitorsSum.size())
                .forEach(i -> museumVisitorsSum.set(i, museumVisitorsSum.get(i).doubleValue() / 1000));
        IntStream.range(0, crimesIncedentsSum.size())
                .forEach(i -> crimesIncedentsSum.set(i, crimesIncedentsSum.get(i).doubleValue() / 1000));
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
//        new SwingWrapper<CategoryChart>(chart).displayChart();
    }

    /***
     *
     * @param museumVisitorsYears
     * @param museumVisitorsSum
     * @param crimesYears
     * @param crimesIncedentsSum
     */
    private static void exportToCSV
    (ArrayList<String> museumVisitorsYears,
     ArrayList<Number> museumVisitorsSum,
     ArrayList<String> crimesYears,
     ArrayList<Number> crimesIncedentsSum) throws Exception {
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream("result.csv"), "utf-8"))) {
            if (!museumVisitorsYears.containsAll(crimesYears))
                throw new Exception("Generated data has incompatable years!");

            AtomicInteger counter = new AtomicInteger(0);

            writer.write("Year,LACrimesCount,LAMuseumVisitorsCount");
            writer.write("\n");
            crimesYears.stream()
                    .forEach(year -> {
                        try {

                            writer.write(year + "," + crimesIncedentsSum.get(counter.get()) + "," + museumVisitorsSum.get(counter.get()));
                            writer.write("\n");
                            counter.addAndGet(1);
                        } catch (IOException e) {
                            System.out.println(e.getMessage());
                        }
                    });
        } catch (UnsupportedEncodingException e) {
            System.out.println(e.getMessage());
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
