package org.acme.commands;

import org.acme.beans.Flight;
import org.acme.utils.SparkUtils;
import org.acme.utils.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import picocli.CommandLine.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * List of passengers who have been on more than 3 flights together
 */
@Command(name="flying-together", description="List of passengers who have been on more than 3 flights together")
@Component
public class GetPassengersFlyingTogether implements Runnable{
    /**
     * Logger
     */
    private static final Logger log = LoggerFactory.getLogger(GetPassengersFlyingTogether.class);
    /**
     * Property for the Help menu
     */
    @Option(names = { "-h", "--help" }, usageHelp = true, description = "Usage")
    private boolean helpRequested = false;
    /**
     * Path of a file with flight data
     */
    @Option(names = {"-f", "--flights-file"},
            description = "Provide the flights data [REQUIRED]",
            required = true)
    String inputPath;
    /**
     * Path to a file to write the output
     */
    @Option(names = {"-o", "--output-file"},
            description = "Write the output in a file [OPTIONAL]")
    String outputPath;

    /**
     * Finds the passengers who have been on more than 3 flights together
     */
    @Override
    public void run() {
        log.info("Processing... (it can take 1-2 minutes)");

        Dataset<Flight> flightsDS = SparkUtils.readFlightDataFromCSV(inputPath);
        JavaRDD<Flight> flightsRdd = flightsDS.javaRDD();
        List<Flight> flightList = flightsRdd.collect();

        //Extract the list of flight ids per passenger id
        Map<Integer, List<Integer>> flightPerPassengerId = Utils.flightIdsPerPassengerId(flightList);

        // Iterate over the value entry (list of flight ids) in nested loops. The algorithm is implemented in a method
        // to test it independently
        List<Row> rows4 = new ArrayList<Row>();
        List<Integer> done = new ArrayList<Integer>();
        this.getPassengersFlyingTogetherAlg(flightPerPassengerId, rows4, done, 3);

        //Create the schema for the output table
        StructType schema4 = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("Passenger_1_Id", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Passenger_2_Id", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Number_Of_Flights_Together", DataTypes.LongType, false),
                }
        );

        SparkSession spark = SparkSession.builder().appName("AcmeFlights").master("local").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> q4DF = spark.createDataFrame(rows4, schema4);

        if (outputPath==null) {
            //Write to console
            q4DF.sort(q4DF.col("Number_Of_Flights_Together").desc(),
                    q4DF.col("Passenger_1_Id").desc(),
                    q4DF.col("Passenger_2_Id").desc()).show(50,false);
        }
        else{
            //Write to file
            this.writeToFile(q4DF,outputPath);
            log.info("Data processed. Results written to path:" + outputPath);
        }
        spark.stop();

    }


    /**
     * @param ds Data rows
     * @param outputPath File path to write the data
     */
    private void writeToFile (Dataset<Row> ds, String outputPath){
        ds.sort(ds.col("Number_Of_Flights_Together").desc(),
                ds.col("Passenger_1_Id").desc(),
                ds.col("Passenger_2_Id").desc()).write().option("header",true).csv(outputPath);
    }

    /**
     * Implements the main algorithm of the command
     * @param flightPerPassengerId
     * @param rows4
     * @param done
     */
    public void getPassengersFlyingTogetherAlg(Map<Integer, List<Integer>> flightPerPassengerId, List<Row> rows4, List<Integer> done, int limit){
        // NOTE: This implementation is not that performant. A possible alternative using flatMap could bring a more
        // efficient way to process this data. To investigate
        flightPerPassengerId.forEach((passenger1, passenger1Flights) -> {
            flightPerPassengerId.forEach((passenger2, passenger2Flights) -> {
                if (passenger1 != passenger2) {
                    long commonFlights = passenger1Flights.stream()
                            .filter(flight1 -> passenger2Flights.stream()
                                    .anyMatch(flight2 -> flight1 == flight2))
                            .count();
                    if (commonFlights > limit) {
                        if (!done.contains(passenger2)) {
                            rows4.add(RowFactory.create(passenger1, passenger2, commonFlights));
                        }
                    }
                }
            });
            done.add(passenger1);
        });
    }

}
