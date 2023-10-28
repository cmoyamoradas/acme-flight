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
import java.util.*;

/**
 *List of passenger ids with the greatest number of countries visited in without being in the UK
 */
@Command(name="longest-run", description="List of passenger ids with the greatest number of countries visited in without being in the UK")
@Component
public class GetLongestRun implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(GetLongestRun.class);
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
     * Finds the greatest number of countries a passenger has been in without being in the UK.
     */
    @Override
    public void run() {
        log.info("Processing...");

        Dataset<Flight> flightsDS = SparkUtils.readFlightDataFromCSV(inputPath);
        JavaRDD<Flight> flightsRdd = flightsDS.javaRDD();
        List<Flight> flightList = flightsRdd.collect();

        // Ensure that the flights are ordered by date, ascending
        // To avoid java.land.UnsupportedOperationException we need to create a new List<Flight> from the one returned by the collect()
        // JavaRDD method
        flightList = new ArrayList<Flight>(flightList);
        Utils.orderFlightsByDate(flightList);

        //Extract the longest route per passenger
        Map<Integer,Integer> longestRoutePerPassenger = Utils.getLongestRoutePerPassengerWithCondition(flightList, "uk");
        //Order the map by value, descending
        Map<Integer,Integer> orderedLongestRoutePerPassenger = Utils.orderEntriesByValue(longestRoutePerPassenger);

        //Build the output table
        List<Row> rows3 = new ArrayList<Row>();
        for (Map.Entry<Integer,Integer> entry : orderedLongestRoutePerPassenger.entrySet()){
            rows3.add(RowFactory.create(entry.getKey(),entry.getValue()));
        }

        StructType schema3 = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("Passenger_Id", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Longest_run", DataTypes.IntegerType, false)
                }
        );

        SparkSession spark = SparkSession.builder().appName("AcmeFlights").master("local").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> q3DF = spark.createDataFrame(rows3, schema3);
        q3DF.sort(q3DF.col("Longest_run").desc());

        if (outputPath==null) {
            //Output to console
            q3DF.show(50,false);
        }
        else{
            //Output to file
            this.writeToFile(q3DF,outputPath);
        }
    }

    /**
     * @param ds Data rows
     * @param outputPath File path to write the data
     */
    private void writeToFile (Dataset<Row> ds, String outputPath){
        ds.write().option("header",true).csv(outputPath);
        log.info("Data processed. Results written in the path: " + outputPath);
    }
}
