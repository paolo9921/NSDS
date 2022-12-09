package it.polimi.nsds.spark.eval;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * Group number:
 * Group members:
 * member 1
 * member 2
 * member 3
 */
public class SparkGroupXX {
    public static void main(String[] args) throws TimeoutException {

        System.setProperty("hadoop.home.dir", "C:\\NSDS\\spark\\spark-3.3.1-bin-hadoop3\\");


        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("SparkEval")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> citiesRegionsFields = new ArrayList<>();
        citiesRegionsFields.add(DataTypes.createStructField("city", DataTypes.StringType, false));
        citiesRegionsFields.add(DataTypes.createStructField("region", DataTypes.StringType, false));
        final StructType citiesRegionsSchema = DataTypes.createStructType(citiesRegionsFields);

        final List<StructField> citiesPopulationFields = new ArrayList<>();
        citiesPopulationFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
        citiesPopulationFields.add(DataTypes.createStructField("city", DataTypes.StringType, false));
        citiesPopulationFields.add(DataTypes.createStructField("population", DataTypes.IntegerType, false));
        final StructType citiesPopulationSchema = DataTypes.createStructType(citiesPopulationFields);

        final Dataset<Row> citiesPopulation = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .schema(citiesPopulationSchema)
                .csv(filePath + "files/cities_population.csv");

        final Dataset<Row> citiesRegions = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .schema(citiesRegionsSchema)
                .csv(filePath + "files/cities_regions.csv");



        /*final Dataset<Row> q1 = citiesRegions
                .join(citiesPopulation, citiesPopulation.col("city").equalTo(citiesRegions.col("city")))
                .groupBy("region").sum("population");*/
        //q1.show();

        // compute the number of cities and the population of the most populated city for each region
        final Dataset<Row> q2 = citiesPopulation.withColumnRenamed("city","city_to_join")
                .join(citiesRegions, col("city_to_join").equalTo(citiesRegions.col("city")))
                .groupBy("region").agg(count(col("city")), max("population"))
                .orderBy(col("count(city)"))
                ;

        q2.cache();
        q2.show();

        // JavaRDD where each element is an integer and represents the population of a city
       /* JavaRDD<Integer> population = citiesPopulation.toJavaRDD().map(r -> r.getInt(2));

        int sum;
        int iteration = 0;
        int treshold = 100000000;

        sum = sumPopulation(population);

        while (sum < treshold){
            iteration++;
            population = population.map( SparkGroupXX::updatePopulation );
            population.cache();
            sum = sumPopulation(population);

            System.out.println("Year: " + iteration + "  total population: " +sum);

        }*/


        // Bookings: the value represents the city of the booking
        final Dataset<Row> bookings = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 100)
                .load();




        /*final StreamingQuery q4 = bookings
                .join(q2, bookings.col("value").equalTo(q2.col("id")))
                .groupBy(
                        window(col("timestamp"),"30 seconds", "5 seconds"),
                        col("region"))
                .count()
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try {
            q4.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }*/

        spark.close();
    }

    private static final int sumPopulation(JavaRDD<Integer> population){
        return population.reduce(Integer::sum);
    }

    private static final int updatePopulation(Integer population){

        if (population > 1000)
            return (int)(population*1.1);

        return (int)(population*0.9);
    }
}
