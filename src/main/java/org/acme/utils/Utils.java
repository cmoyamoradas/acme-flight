package org.acme.utils;

import org.acme.beans.Flight;
import org.acme.beans.Passenger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.reverseOrder;

/**
 * Class of utilities for Collection processing
 */
public class Utils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);
    /**
     * @param flights List of org.acme.beans.Flight
     * @return Map<Integer,List<Integer>) with key=passengerId and value=list of flightIds
     */
    public static Map<Integer,List<Integer>> flightIdsPerPassengerId (List<Flight> flights){
        return flights.stream()
                .collect(Collectors.groupingBy(Flight::getPassengerId,Collectors.mapping(Flight::getFlightId,Collectors.toList())));
    }

    /**
     * @param flights List of org.acme.beans.Flight
     * @return Map<Integer,List<Flight>) with key=passengerId and value=list of org.acme.beans.Flight
     */
    public static Map<Integer,List<Flight>> flightsPerPassengerId (List<Flight> flights){
        return flights.stream()
                .collect(Collectors.groupingBy(Flight::getPassengerId,Collectors.toList()));
    }

    /**
     * @param flights List of org.acme.beans.Flight
     * @param condition Condition to split the routes, referred to from|to values of a flight
     * @return Map<Integer,Integer> with key=passengerId and value=max number of countries visited considering the split condition
     */
    public static Map<Integer,Integer> getLongestRoutePerPassengerWithCondition(List<Flight> flights, String condition){
        //Create a map of passengers and their flights as a List<Flight>
        Map<Integer, List<Flight>> flightPerPassengerId = flights.stream()
                .collect(Collectors.groupingBy(Flight::getPassengerId,Collectors.toList()));

        //Transform the flights into a list of from,to
        Map<Integer, List<String>> routesPerPassengerId = flightPerPassengerId.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,e-> e.getValue().stream().collect(Collectors.mapping(
                                p -> {
                                    List<String> list = new ArrayList<String>();
                                    list.add(p.getFrom());
                                    list.add(p.getTo());
                                    return list;
                                },
                                Collectors.toList())).stream().flatMap(list -> list.stream())
                        .collect(Collectors.toList())));

        //Transform the routes from "from,to" to single country
        Map<Integer, List<String>> routesTransformed = routesPerPassengerId.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(),e -> Utils.transformRoutesFromToIntoSingleCountry(e.getValue())));

        //Split the routes in several lists of routes, by the condition
        Map<Integer, List<List<String>>> routesTransformed2 = routesTransformed.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(),e -> {
                    return Utils.splitRoutesListByValue(e.getValue(),condition);
                }));

        //Calculate the longest route
        Map<Integer, Integer> longestRoutePerPassenger = routesTransformed2.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(),e -> {
                    return Utils.getLongestRoute(e.getValue());
                }));

        return longestRoutePerPassenger;
    }

    /**
     * @param unorderedMap Unordered Map<Integer,Integer> with key=an int value and value=an int value
     * @return Map<Integer,Integer> ordered by value
     */
    public static Map<Integer,Integer> orderEntriesByValue(Map<Integer,Integer> unorderedMap){
       return unorderedMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    /**
     * @param passengerKeys Passenger ids for which to filter
     * @param listToFilter List of org.acme.beans.Passenger to filter
     * @return Filtered list of org.acme.beans.Passenger
     */
    public static List<Passenger> getFilteredListOfPassengers(Set<Integer> passengerKeys, List<Passenger> listToFilter){
        return  listToFilter.stream()
                .filter(passenger -> passengerKeys.contains(passenger.getPassengerId()))
                .collect(Collectors.toList());
    }

    /**
     * @param flights List of org.acme.beans.Flight
     * @return Map<Integer,Long> with key=passengerId and value=total number of flights for each passenger
     */
    public static Map<Integer,Long> flightsPerPassenger (List<Flight> flights){
        Map<Integer, Long> flightsPerPassenger = flights.stream()
                .collect(Collectors.groupingBy(Flight::getPassengerId,Collectors.counting()))
                .entrySet()
                .stream()
                .sorted(Map.Entry.<Integer, Long>comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        return flightsPerPassenger;
    }

    /**
     * @param unorderedMap
     * @param limit Number of entries of the input map to return
     * @return Map<Integer,Long> ordered by value
     */
    public static Map<Integer,Long> extractFirstNEntriesOrderedByValue (Map<Integer,Long> unorderedMap, int limit){
        return unorderedMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(limit)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    /**
     * @param flights List of org.acme.beans.Flight
     * @return Map<Integer,Long> with key=year month and value=total number of flights
     */
    public static Map<Integer,Long> getNumberOfFlightsByMonth(List<Flight> flights){

        Map<Integer, Long> flightsPerMonth = flights.stream()
                .collect(Collectors.groupingBy(Flight::getMonth,Collectors.counting()));

        return flightsPerMonth;
    }

    /**
     * Clean up the list transforming the adjacent duplicates from,to into a single country
     *
     * @param input List of countries, as part of a route made up of from and to, ordered
     * @return List of countries simplified, ordered
     */
    public static List<String> transformRoutesFromToIntoSingleCountry (List<String> input){
        List<String> output = new ArrayList<String>();
        Iterator<String> it = input.iterator();
        int index = 0;
        while(it.hasNext()){
            String country = it.next();
            if (output.isEmpty() || !country.equals(output.get(index-1)) || !it.hasNext()){
                output.add(country);
                index++;
            }
        }
        return output;
    }

    /**
     * @param input List of simplified routes, ordered
     * @param value Condition to split the routes, expressed as a country.
     * @return
     */
    public static List<List<String>> splitRoutesListByValue(List<String> input, String value){
        List<List<String>> output = new ArrayList<>();
        input.forEach(w -> {
            if(w.equals(value)) {
                List<String> bucket = new ArrayList<>();
                output.add(bucket);
            }
            else if (output.isEmpty()){
                List<String> bucket = new ArrayList<>();
                bucket.add(w);
                output.add(bucket);
            }
            else {
                // not contains uk put the value in the last bucket
                output.get(output.size() - 1).add(w);
            }
        });
        return output;
    }

    /**
     * @param input List of routes, expressed as a list of countries
     * @return Max number of countries found among all the routes
     */
    public static int getLongestRoute(List<List<String>> input){
        return input.stream().mapToInt(x -> x.size()).max().getAsInt();
    }

    /**
     * @param flights List of flights to order
     */
    public static void orderFlightsByDate(List<Flight> flights){
        Collections.sort(flights, new Comparator<Flight>() {
            @Override
            public int compare(Flight f1, Flight f2) {
                return f1.getDate().compareTo(f2.getDate());
            }
        });
    }

}
