package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.security.acl.Group;
import java.util.*;

import static simpledb.execution.Aggregator.Op.MAX;
import static simpledb.execution.Aggregator.Op.MIN;
import static simpledb.execution.Aggregator.Op.SUM;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 * 我这种思路算avg的话会有问题，因为本身整数除法会舍掉一部分,一个一个merge的话，再乘以个数与原本的Sum是有出入的
 * 所以不得已再加两个关于Sum的变量
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     * the 0-based index of the group-by field in the tuple, or
     * NO_GROUPING if there is no grouping
     * @param gbfieldtype
     * the type of the group by field (e.g., Type.INT_TYPE), or null
     * if there is no grouping
     * @param afield
     * the 0-based index of the aggregate field in the tuple
     * @param what
     * the aggregation operator
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private List<Tuple> result = new ArrayList<>();
    private int resultCount = 0;
    private int resultSum = 0;
    private Map<Field, Tuple> resultGroup = new HashMap<>();
    private Map<Field, Integer> resultGroupCount = new HashMap<>();
    private Map<Field, Integer> resultGroupSum = new HashMap<>();
    private TupleDesc tupleDesc;
    private IntField newField;
    private Iterable<Tuple> it;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield == Aggregator.NO_GROUPING) {
            Type[] types = new Type[]{Type.INT_TYPE};
            String[] strs = new String[]{"aggName(" + what.toString() + ")"};
            tupleDesc = new TupleDesc(types, strs);
        } else {
            Type[] typesGroup = new Type[]{gbfieldtype, Type.INT_TYPE};
            String[] strs = new String[]{"groupName", "aggName(" + what.toString() + ")"};
            tupleDesc = new TupleDesc(typesGroup, strs);
        }
    }


    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField aggregate = (IntField) tup.getField(afield);
        Tuple tuple = new Tuple(tupleDesc);
        if (gbfield == Aggregator.NO_GROUPING) {
            tuple.setField(0, aggregate);
            tuple = aggregateTuplesGroup(resultCount, resultSum, tuple, tuple, 0);
            result.clear();
            result.add(tuple);
            resultCount++;
            resultSum += aggregate.getValue();
        } else {
            Field group = tup.getField(gbfield);
            tuple.setField(0, group);
            tuple.setField(1, aggregate);
            if (resultGroup.containsKey(group)) {
                int count = resultGroupCount.get(group);
                int sum = resultGroupSum.get(group);
                Tuple tupleOrigin = resultGroup.get(group);
                tuple = aggregateTuplesGroup(count, sum, tupleOrigin, tuple, 1);
            }
            //因为我是取出tuple再修改,第一次Count的时候不会调用,手动修改tuple,这里的tupleOrigin无用，直接赋null
            if (!resultGroup.containsKey(group) && what == Op.COUNT) {
                tuple = aggregateTuplesGroup(0, 0, tuple, tuple, 1);
            }
            resultGroup.put(group, tuple);
            resultGroupCount.put(group, resultGroupCount.getOrDefault(group, 0) + 1);
            resultGroupSum.put(group, resultGroupSum.getOrDefault(group, 0) + aggregate.getValue());
        }
    }


    public Tuple aggregateTuplesGroup(int count, int sum, Tuple tupleOrigin, Tuple tuple, int index) {
        IntField fieldOrigin = (IntField) tupleOrigin.getField(index);
        IntField field = (IntField) tuple.getField(index);
        int origin = fieldOrigin.getValue();
        int val = field.getValue();
        switch (what) {
            case SUM:
                newField = new IntField(origin + val);
                tuple.setField(index, newField);
                break;
            case MAX:
                newField = new IntField(Math.max(origin, val));
                tuple.setField(index, newField);
                break;
            case MIN:
                newField = new IntField(Math.min(origin, val));
                tuple.setField(index, newField);
                break;
            case AVG:
                newField = new IntField((sum+val) / (count + 1));
                tuple.setField(index, newField);
                break;
            case COUNT:
                newField = new IntField(count + 1);
                tuple.setField(index, newField);
                break;
        }
        return tuple;
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if (gbfield == Aggregator.NO_GROUPING) {
            it = result;
        } else {
            it = resultGroup.values();
        }
        OpIterator iterator = new TupleIterator(tupleDesc, it);
        return iterator;
    }

}
