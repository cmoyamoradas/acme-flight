package org.acme.commands;

import org.acme.beans.Flight;
import org.acme.beans.Passenger;
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
import java.util.*;

/**
 * List of the 100 most frequent flyers
 */
@Command(name="frequent-flyers", description="List of the 100 most frequent flyers")
@Component
public class GetMostFrequentFlyers implements Runnable {
    /**
     * Logger
     */
    private static final Logger log = LoggerFactory.getLogger(GetMostFrequentFlyers.class);
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
    String inputFlightsPath;
    /**
     * Path to a file with passenger data
     */
    @Option(names = {"-p", "--passengers-file"},
            description = "Provide the passengers data [REQUIRED]",
            required = true)
    String inputPassengersPath;
    /**
     * Path to a file to write the output
     */
    @Option(names = {"-o", "--output-file"},
            description = "Write the output in a file [OPTIONAL]")
    String outputPath;

    /**
     * Finds the names of the 100 most frequent flyers
     */
    @Override
    public void run() {
        log.info("Processing...");

        Dataset<Passenger> passengerDS = SparkUtils.readPassengerDataFromCSV(inputPassengersPath);
        Dataset<Flight> flightsDS = SparkUtils.readFlightDataFromCSV(inputFlightsPath);
        JavaRDD<Flight> flightsRdd = flightsDS.javaRDD();

        List<Flight> flightList = flightsRdd.collect();
        //EXtract the total number of flights per passenger id
        Map<Integer,Long> flightsPerPassenger = Utils.flightsPerPassenger(flightList);
        //Return the 100 first entries ordered by value (number of flights, in descending order)
        flightsPerPassenger = Utils.extractFirstNEntriesOrderedByValue(flightsPerPassenger,100);

        JavaRDD<Passenger> passengersRdd = passengerDS.javaRDD();
        List<Passenger> passengersList = passengersRdd.collect();
        Set<Integer> passKeySet = flightsPerPassenger.keySet();
        //Intersect the entire list of passengers and the list of most frequent flyers, to subsequently extract the
        //first and last names and construct the table for the output
        passengersList = Utils.getFilteredListOfPassengers(passKeySet,passengersList);
        List<Row> rows2 = new ArrayList<Row>();
        for (Map.Entry<Integer,Long> entry : flightsPerPassenger.entrySet()){
            Passenger passenger = passengersList.stream().filter(pass -> pass.getPassengerId()== entry.getKey()).findFirst().orElse(null);
            rows2.add(RowFactory.create(entry.getKey(),entry.getValue(),passenger.getFirstName(),passenger.getLastName()));
        }

        StructType schema2 = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("Passenger_Id", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Number_of_flights", DataTypes.LongType, false),
                        DataTypes.createStructField("First_name", DataTypes.StringType, false),
                        DataTypes.createStructField("Last_name", DataTypes.StringType, false)
                }
        );

        SparkSession spark = SparkSession.builder().appName("AcmeFlights").master("local").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> q2DF = spark.createDataFrame(rows2, schema2);

        if (outputPath==null) {
            q2DF.show(50,false);
        }
        else{
            this.writeToFile(q2DF,outputPath);
            log.info("Data processed. Results written in the path:" + outputPath);
        }
        spark.stop();
    }

    /**
     * @param ds Data rows
     * @param outputPath File path to write the data
     */
    private void writeToFile (Dataset<Row> ds, String outputPath){
        ds.write().option("header",true).csv(outputPath);
    }
}
