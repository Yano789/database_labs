package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.TupleIterator;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private boolean hasGroup;
    private Map<Field, Integer> aggregateResults;

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

        hasGroup = this.gbfield != NO_GROUPING;
        this.aggregateResults = new HashMap<>();
        if (!this.what.equals(Op.COUNT)) {
            throw new IllegalArgumentException("StringAggregator only support COUNT");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field fieldToGroup = new IntField(NO_GROUPING);
        if (hasGroup) {
            fieldToGroup = tup.getField(this.gbfield);
        }
        
        String newValue = ((StringField) tup.getField(this.afield)).getValue();
        int oldValue = this.aggregateResults.getOrDefault(fieldToGroup, 0);
        this.aggregateResults.put(fieldToGroup, oldValue + 1);
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
