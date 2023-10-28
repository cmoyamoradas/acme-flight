package org.acme.commands;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class GetPassengersFlyingTogetherTest {
    @Test
    public void getPassengersFlyingTogetherAlgSuccess(){
        Map<Integer,List<Integer>> input = new HashMap<Integer,List<Integer>>();
        input.put(1, Arrays.asList(new Integer[]{1,9,12}));
        input.put(2, Arrays.asList(new Integer[]{8,3,12,15}));
        input.put(3, Arrays.asList(new Integer[]{8,12,14,15}));
        input.put(4, Arrays.asList(new Integer[]{6,10}));
        input.put(5, Arrays.asList(new Integer[]{12,15}));
        input.put(6, Arrays.asList(new Integer[]{4,9,10}));

        List<Row> expected = new ArrayList<Row>();
        expected.add(RowFactory.create(2, 3, 3));
        expected.add(RowFactory.create(2, 5, 2));
        expected.add(RowFactory.create(3, 5, 2));

        List<Row> actual = new ArrayList<Row>();

        GetPassengersFlyingTogether command = new GetPassengersFlyingTogether();
        command.getPassengersFlyingTogetherAlg(input,actual,new ArrayList<Integer>(),1);

        assertFalse(actual.isEmpty());
        assertTrue(actual.equals(expected));

    }
}
