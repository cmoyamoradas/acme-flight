package org.acme.utils;

import org.acme.beans.Flight;
import org.acme.beans.Passenger;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class of utilities for Spark processing
 */
public class SparkUtils {
    /**
     * Logger
     */
    private static final Logger log = LoggerFactory.getLogger(SparkUtils.class);
    /**
     * @param path Path to a file with passenger's data
     * @return Spark SQL Dataset with org.acme.beans.Passenger values
     */
    public static Dataset<Passenger> readPassengerDataFromCSV(String path){
        SparkSession spark = SparkSession.builder().appName("AcmeFlights").master("local").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> passengersDs = spark.read().option("delimiter",",")
                .option("header","true")
                .csv(path);

        Dataset<Passenger> passengersDs2 = passengersDs.withColumn("passengerId", passengersDs.col("passengerId").cast("int"))
                .as(Encoders.bean(Passenger.class));

        return passengersDs2;
    }

    /**
     * @param path Path to a file with flight's data
     * @return Spark SQL Dataset with org.acme.beans.Flight values
     */
    public static Dataset<Flight> readFlightDataFromCSV(String path){
        SparkSession spark = SparkSession.builder().appName("AcmeFlights").master("local").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> flightsDs = spark.read().option("delimiter",",")
                .option("header","true")
                .csv(path);

        Dataset<Flight> flightsDs2 = flightsDs.withColumn("flightId", flightsDs.col("flightId").cast("int"))
                .withColumn("passengerId", flightsDs.col("passengerId").cast("int"))
                .withColumn("date", functions.to_date(flightsDs.col("date"),"yyyy-MM-dd"))
                .as(Encoders.bean(Flight.class));

        return flightsDs2;
    }
}
