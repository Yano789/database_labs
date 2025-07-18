package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.TupleIterator;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;


/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> aggregateResults;
    private Map<Field, Integer> countByField;
    private boolean hasGroup;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        hasGroup = this.gbfield != NO_GROUPING;
        this.aggregateResults = new HashMap<>();
        if (this.what.equals(Op.AVG)) {
            this.countByField = new HashMap<>();
        }
    }


    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field fieldToGroup = new IntField(NO_GROUPING);
        if (hasGroup) {
            fieldToGroup = tup.getField(this.gbfield);
        }

        int newValue = ((IntField) tup.getField(this.afield)).getValue();
        switch (this.what) {
            case AVG:
                int prevSum = this.aggregateResults.getOrDefault(fieldToGroup, 0);
                int prevCount = this.countByField.getOrDefault(fieldToGroup, 0);
                int newSum = prevSum + newValue;
                aggregateResults.put(fieldToGroup, newSum);
                countByField.put(fieldToGroup, prevCount + 1);
                break;
            case MAX:
                int oldValue = this.aggregateResults.getOrDefault(fieldToGroup, Integer.MIN_VALUE);
                aggregateResults.put(fieldToGroup, Math.max(newValue, oldValue));
                break;
            case MIN:
                oldValue = this.aggregateResults.getOrDefault(fieldToGroup, Integer.MAX_VALUE);
                aggregateResults.put(fieldToGroup, Math.min(newValue, oldValue));
                break;
            case SUM:
                oldValue = this.aggregateResults.getOrDefault(fieldToGroup, 0);
                aggregateResults.put(fieldToGroup, oldValue + newValue);
                break;
            case COUNT:
                oldValue = this.aggregateResults.getOrDefault(fieldToGroup, 0);
                aggregateResults.put(fieldToGroup, oldValue + 1);
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // throw new
        // UnsupportedOperationException("please implement me for lab2");
        TupleDesc td;
        if (hasGroup) {
            td = new TupleDesc(new Type[] {
                this.gbfieldtype,
                Type.INT_TYPE
            });
        }
        else {
            td = new TupleDesc(new Type[] {
                Type.INT_TYPE
            });
        }

        List<Tuple> tuples = getTuplesForAggregatedResults(td);
        return new TupleIterator(td, tuples);
    }

    private List<Tuple> getTuplesForAggregatedResults(TupleDesc td) {
        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry: this.aggregateResults.entrySet()) {
        Tuple tup = new Tuple(td);
        int aggregateValue = entry.getValue();
        if (this.what == Op.AVG) {
            int count = this.countByField.get(entry.getKey());
            aggregateValue /= count;
        }
        if (hasGroup) {
            tup.setField(0, entry.getKey());
            tup.setField(1, new IntField(aggregateValue));
        } else {
            tup.setField(0, new IntField(aggregateValue));
        }
        tuples.add(tup);
        }
        return tuples;
  }

}
