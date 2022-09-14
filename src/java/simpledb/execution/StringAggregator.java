package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private List<Tuple> result = new ArrayList<>();
    private int resultCount = 0;
    private Map<Field, Tuple> resultGroup = new HashMap<>();
    private Map<Field, Integer> resultGroupCount = new HashMap<>();
    private TupleDesc tupleDesc;
    private IntField newField;
    private Iterable<Tuple> it;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield == Aggregator.NO_GROUPING) {
            Type[] types = new Type[]{Type.INT_TYPE};
            String[] strs=new String[]{"aggName("+what.toString()+")"};
            tupleDesc = new TupleDesc(types);
        } else {
            Type[] typesGroup = new Type[]{gbfieldtype, Type.INT_TYPE};
            String[] strs=new String[]{"groupName","aggName("+what.toString()+")"};
            tupleDesc = new TupleDesc(typesGroup);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField aggregate = (StringField) tup.getField(afield);
        Tuple tuple = new Tuple(tupleDesc);
        if (gbfield == Aggregator.NO_GROUPING) {
            Tuple tupleOrigin=result.isEmpty()?null:result.get(0);
            tuple = aggregateTuplesGroup(resultCount, tupleOrigin, tuple,0);
            result.clear();
            result.add(tuple);
            resultCount++;
        } else {
            Field group = tup.getField(gbfield);
            tuple.setField(0, group);
            int count = resultGroupCount.getOrDefault(group,0);
            Tuple tupleOrigin = resultGroup.getOrDefault(group,null);
            tuple = aggregateTuplesGroup(count, tupleOrigin, tuple,1);
            resultGroup.put(group, tuple);
            resultGroupCount.put(group, resultGroupCount.getOrDefault(group, 0) + 1);
        }
    }

    public Tuple aggregateTuplesGroup(int count, Tuple tupleOrigin, Tuple tuple,int index) {
        switch (what) {
            case COUNT:
                newField = new IntField(count+1);
                tuple.setField(index, newField);
                break;
        }
        return tuple;
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
        if (gbfield == Aggregator.NO_GROUPING) {
            it = result;
        } else {
            it = resultGroup.values();
        }
        OpIterator iterator=new TupleIterator(tupleDesc,it);
        return iterator;
    }

}
