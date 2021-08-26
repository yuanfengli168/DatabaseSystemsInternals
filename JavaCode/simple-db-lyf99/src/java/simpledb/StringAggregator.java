package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // Jacky's field
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> group;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.group = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // groupFieldValue is the Field value of the field that grouped by upon
        Field groupFieldValue = null;
        if (this.gbfield != NO_GROUPING) {
            groupFieldValue = tup.getField(gbfield);
        }
        if (group.containsKey(groupFieldValue)) {
            group.put(groupFieldValue, group.get(groupFieldValue) + 1);
        } else {
            group.put(groupFieldValue, 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> list = new ArrayList<>();
        if (this.gbfield == NO_GROUPING) {
            TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aValue"});
            int aValue = group.get(null);
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(aValue));
            list.add(tuple);
            return new TupleIterator(td, list);
        } else {
            TupleDesc td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},  new String[]{"groupValue", "aggregateValue"});
            for (Field groupValue : group.keySet()) {
                int aValue = group.get(groupValue);
                Tuple tuple = new Tuple(td);
                tuple.setField(0, groupValue);
                tuple.setField(1, new IntField(aValue));
                list.add(tuple);
            }
            return new TupleIterator(td, list);
        }
    }

}
