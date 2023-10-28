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
 * Total number of flights per year month
 */
@Command(name = "flights-per-month", description="Total number of flights per year month")
@Component
public class GetNumberOfFlightsPerMonth implements Runnable {
    /**
     * Logger
     */
    private static final Logger log = LoggerFactory.getLogger(GetNumberOfFlightsPerMonth.class);
    /**
     * Property for the Help menu of the command
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
     * Finds the total number of flights for each month
     */
    @Override
    public void run() {
        log.info("Processing...");

        Dataset<Flight> flightsDS = SparkUtils.readFlightDataFromCSV(inputPath);
        JavaRDD<Flight> flightsRdd = flightsDS.javaRDD();
        List<Flight> flightList = flightsRdd.collect();

        //Extract the number of flights per each year month
        Map<Integer, Long> flightsPerMonth = Utils.getNumberOfFlightsByMonth(flightList);

        //Construct the table for the output
        StructType schema1 = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("Month", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Number_of_flights", DataTypes.LongType, false)
                }
        );
        List<Row> rows1 = new ArrayList<Row>();
        for (Map.Entry<Integer,Long> entry : flightsPerMonth.entrySet()){
            rows1.add(RowFactory.create(entry.getKey(),entry.getValue()));
        }

        SparkSession spark = SparkSession.builder().appName("AcmeFlights").master("local").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        Dataset<Row> ds = spark.createDataFrame(rows1, schema1);

        if (outputPath==null) {
            //Write to console
            ds.show(50,false);
        }
        else{
            //Write to file
            this.writeToFile(ds,outputPath);
            log.info("Data processed. Results written to path: " + outputPath);
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
