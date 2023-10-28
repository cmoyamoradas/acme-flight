package org.acme.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import picocli.CommandLine.*;

/**
 * Parent command that does nothing
 */
@Command(description="Acme Flights is an application that processes flight data", subcommands = {
        GetLongestRun.class,
        GetNumberOfFlightsPerMonth.class,
        GetPassengersFlyingTogether.class,
        GetMostFrequentFlyers.class
})
@Component
public class FlightCommand implements Runnable {
    /**
     * Logger
     */
    private static final Logger log = LoggerFactory.getLogger(FlightCommand.class);
    /**
     * Property for the Help menu
     */
    @Option(names = { "-h", "--help" }, usageHelp = true, description = "Usage")
    private boolean helpRequested = false;
    /**
     * Does nothing. Just prints a message
     */
    @Override
    public void run() {
        log.info("Flight command does nothing. Add -h option to list the available subcommands");
    }
}
