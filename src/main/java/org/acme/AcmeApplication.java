package org.acme;

import org.acme.commands.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import picocli.CommandLine;

/**
 * Acme Application main class
 *
 * Acme Flight is an application that processes some flight data to provide the different results.
 */
@SpringBootApplication
public class AcmeApplication implements CommandLineRunner{
    /**
     * Logger
     */
    private static final Logger log = LoggerFactory.getLogger(AcmeApplication.class);
    /**
     * Parent command that does nothing
     */
    private FlightCommand flightCommand;
    /**
     * Command that gets the list of passenger ids with the greatest number of countries visited in without being in the UK
     */
    private GetLongestRun longestRunCommand;
    /**
     * Command that gets the list of 100 most frequent flyers
     */
    private GetMostFrequentFlyers frequentFlyersCommand;
    /**
     * Command that gets the total number of flights per year month
     */
    private GetNumberOfFlightsPerMonth flightsPerMonthCommand;
    /**
     * Command that gets the list of passengers who have been on more than 3 flights together
     */
    private GetPassengersFlyingTogether flyingTogetherCommand;

    /**
     * Constructor
     *
     * @param flightCommand Parent command that does nothing
     * @param longestRunCommand List of passenger ids with the greatest number of countries visited in without being in the UK
     * @param frequentFlyersCommand List of 100 most frequent flyers
     * @param flightsPerMonthCommand Total number of flights per year month
     * @param flyingTogetherCommand List of passengers passengers who have been on more than 3 flights together
     */
    @Autowired
    public AcmeApplication (FlightCommand flightCommand, GetLongestRun longestRunCommand,
                                        GetMostFrequentFlyers frequentFlyersCommand, GetNumberOfFlightsPerMonth flightsPerMonthCommand,
                                        GetPassengersFlyingTogether flyingTogetherCommand) {
        this.flightCommand = flightCommand;
        this.longestRunCommand = longestRunCommand;
        this.frequentFlyersCommand = frequentFlyersCommand;
        this.flightsPerMonthCommand = flightsPerMonthCommand;
        this.flyingTogetherCommand = flyingTogetherCommand;
    }

    /**
     * @param args Application arguments
     * @throws Exception
     */
    public static void main(String[] args) {
        try {
            SpringApplication.run(AcmeApplication.class, args);
        } catch(Throwable th){
            th.printStackTrace();
        }
    }

    /**
     * @param args Arguments to pass to the commands
     * @throws Exception
     */
    @Override
    public void run(String... args) throws Exception {

        CommandLine commandLine = new CommandLine(flightCommand);
        commandLine.parseWithHandler(new CommandLine.RunLast(), args);
    }

}
