package org.acme.beans;

import java.io.Serializable;

/**
 * Bean that represents passenger data
 */
public class Passenger implements Serializable {
    /**
     * @return Passenger identifier
     */
    public int getPassengerId() {return passengerId;}

    /**
     * @return First name of the passenger
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * @return Last name of the passenger
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * Passenger Identifier
     */
    private  int passengerId;

    /**
     * @param firstName First name of the passenger
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * @param passengerId Passenger identifier
     */
    public void setPassengerId(int passengerId) {
        this.passengerId = passengerId;
    }

    /**
     * @param lastName Last name of the passenger
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /**
     * First name of the passenger
     */
    private  String firstName;
    /**
     * Last name of the passenger
     */
    private  String lastName;

    /**
     * Default constructor
     */
    public Passenger(){}

    public Passenger(int passengerId, String firstName, String lastName){
        this.passengerId = passengerId;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    /**
     * @return String representation of the passenger
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[")
                .append(this.passengerId).append("|")
                .append(this.firstName).append("|")
                .append(this.lastName)
                .append("]");
        return sb.toString();
    }
}
