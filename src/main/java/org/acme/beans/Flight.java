package org.acme.beans;

import java.io.Serializable;
import java.sql.Date;
import java.util.Calendar;

/**
 * Bean that represents flight data
 */
public class Flight implements Serializable {
    /**
     * @return passenger identifier
     */
    public int getPassengerId() {return passengerId;}

    /**
     * @return fiight identifier
     */
    public int getFlightId() {
        return flightId;
    }

    /**
     * @return Country origin of flight
     */
    public String getFrom() {
        return from;
    }

    /**
     * @return Country flight destination
     */
    public String getTo() {
        return to;
    }

    /**
     * @return Date of the flight
     */
    public Date getDate() {
        return date;
    }

    /**
     * Passenger identifier
     */
    private int passengerId;
    /**
     * Flight identifier
     */
    private int flightId;
    /**
     * Country flight destination
     */
    private String from;

    /**
     * @param passengerId Passenger identifier
     */
    public void setPassengerId(int passengerId) {
        this.passengerId = passengerId;
    }

    /**
     * @param flightId Flight identifier
     */
    public void setFlightId(int flightId) {
        this.flightId = flightId;
    }

    /**
     * @param from Country origin of flight
     */
    public void setFrom(String from) {
        this.from = from;
    }

    /**
     * @param to Country flight destination
     */
    public void setTo(String to) {
        this.to = to;
    }

    /**
     * @param date Date of the flight
     */
    public void setDate(Date date) {
        this.date = date;
    }

    /**
     * Country flight destination
     */
    private String to;
    /**
     * Date of the flight
     */
    private Date date;

    /**
     * Default constructor
     */
    public Flight(){}

    public Flight(int passengerId, int flightId, String from, String to, Date date){
        this.passengerId = passengerId;
        this.flightId = flightId;
        this.from = from;
        this.to = to;
        this.date = date;
    }

    /**
     * @return String representation of the flight
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[")
                .append(this.flightId)
                .append(this.passengerId).append("|")
                .append(this.from).append("|")
                .append(this.to).append("|")
                .append(this.date)
                .append("]");
        return sb.toString();
    }

    /**
     * @return Month of the flight date
     */
    public int getMonth (){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(this.date);
        return calendar.get(Calendar.MONTH)+1;
    }
}
