package org.acme.utils;

import org.acme.beans.Flight;
import org.acme.beans.Passenger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
public class UtilsTest {
    private static final Logger log = LoggerFactory.getLogger(UtilsTest.class);

    private static List<Flight> flights;
    private static List<Passenger> passengers;
    private static Passenger createPassenger(String[] data){
        return new Passenger(Integer.parseInt(data[0]), data[1], data[2]);
    }
    private static Flight createFlight(String[] data){
       return new Flight(Integer.parseInt(data[0]), Integer.parseInt(data[1]), data[2], data[3], Date.valueOf(data[4]));
    }
    @BeforeAll
    public static void init() {
        flights = new ArrayList<Flight>();
        flights.add(createFlight(new String[]{"1","1","cg","ir","2017-01-01"}));
        flights.add(createFlight(new String[]{"2","2","cg","ir","2017-02-01"}));
        flights.add(createFlight(new String[]{"2","3","cg","ir","2017-03-01"}));
        flights.add(createFlight(new String[]{"6","4","cg","ir","2017-03-01"}));
        flights.add(createFlight(new String[]{"5","5","cg","ir","2017-04-01"}));
        flights.add(createFlight(new String[]{"4","6","cg","ir","2017-05-01"}));
        flights.add(createFlight(new String[]{"1","7","cg","ir","2017-06-01"}));
        flights.add(createFlight(new String[]{"3","8","cg","ir","2017-06-01"}));
        flights.add(createFlight(new String[]{"6","9","cg","ir","2017-07-01"}));
        flights.add(createFlight(new String[]{"6","10","cg","ir","2017-08-01"}));
        flights.add(createFlight(new String[]{"1","11","cg","ir","2017-09-01"}));
        flights.add(createFlight(new String[]{"2","12","cg","ir","2017-10-01"}));
        flights.add(createFlight(new String[]{"4","13","cg","ir","2017-11-01"}));
        flights.add(createFlight(new String[]{"3","14","cg","ir","2017-12-01"}));
        flights.add(createFlight(new String[]{"2","15","cg","ir","2017-12-01"}));

        passengers = new ArrayList<Passenger>();
        passengers.add(createPassenger(new String[]{"1","Napoleon","Gaylene"}));
        passengers.add(createPassenger(new String[]{"2","Katherin","Shanell"}));
        passengers.add(createPassenger(new String[]{"3","Stevie","Steven"}));
        passengers.add(createPassenger(new String[]{"4","Margarita","Gerri"}));
        passengers.add(createPassenger(new String[]{"5","Earle","Candis"}));
        passengers.add(createPassenger(new String[]{"6","Trent","Omer"}));

    }

    @Test
    public void getLongestRoutePerPassengerWithConditionSuccess(){
        Map<Integer,List<Integer>> expected = new HashMap<Integer,List<Integer>>();
        expected.put(1, Arrays.asList(new Integer[]{1,7,11}));
        expected.put(2, Arrays.asList(new Integer[]{2,3,12,15}));
        expected.put(3, Arrays.asList(new Integer[]{8,14}));
        expected.put(4, Arrays.asList(new Integer[]{6,13}));
        expected.put(5, Arrays.asList(new Integer[]{5}));
        expected.put(6, Arrays.asList(new Integer[]{4,9,10}));

        Map<Integer,List<Integer>> actual = Utils.flightIdsPerPassengerId(flights);

        assertNotNull(actual);
        assertTrue(actual.equals(expected));
    }
    @Test
    public void flightsPerPassengerSuccess(){
        Map<Integer, Long> expected = new HashMap<Integer,Long>();
        expected.put(1,3L);
        expected.put(2,4L);
        expected.put(3,2L);
        expected.put(4,2L);
        expected.put(5,1L);
        expected.put(6,3L);

        Map<Integer, Long> actual = Utils.flightsPerPassenger(flights);

        assertNotNull(actual);
        assertTrue(actual.equals(expected));

    }
    @Test
    public void transformRoutesFromToIntoSingleCountrySuccess(){
        List<String> input = Arrays.asList(new String[]{"ir","cq","cq","es","es","uk","uk","uk","ge","ge","ir"});
        List<String> expected = Arrays.asList(new String[]{"ir","cq","es","uk","ge","ir"});

        List<String> actual = Utils.transformRoutesFromToIntoSingleCountry(input);

        assertNotNull(actual);
        assertTrue(actual.equals(expected));
    }
    @Test
    public void splitRoutesListByValueSuccess(){
        List<String> input = Arrays.asList(new String[]{"ir","cq","es","uk","de","ir"});
        List<List<String>> expected = new ArrayList<List<String>>();
        expected.add(Arrays.asList(new String[]{"ir","cq","es"}));
        expected.add(Arrays.asList(new String[]{"de","ir"}));

        List<List<String>> actual = Utils.splitRoutesListByValue(input,"uk");

        System.out.println(actual);

        assertNotNull(actual);
        assertTrue(actual.equals(expected));
    }
}
