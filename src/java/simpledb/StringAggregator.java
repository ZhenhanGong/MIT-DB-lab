package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    // TODO:   note: only support count

    private static final long serialVersionUID = 1L;
    private int groupByFieldIndex;
    private Type groupByType;
    private int fieldIndex;
    private Op op;

    // grouping
    private HashMap<Field, Integer> aggrResult;

    // no grouping
    private boolean no_grouping;
    private int aggrResult2;

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
        if (gbfield == Aggregator.NO_GROUPING) {
            no_grouping = true;
        }
        groupByFieldIndex = gbfield;
        groupByType = gbfieldtype;
        fieldIndex = afield;
        op = what;
        aggrResult = new HashMap<>();
        aggrResult2 = 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // TODO note: only support count
        if (no_grouping) {
            aggrResult2++;

        } else {
            Field groupByField = tup.getField(groupByFieldIndex);

            // need to create a new group
            if (!aggrResult.containsKey(groupByField)) {
                aggrResult.put(groupByField, 1);
            } else {
                int count = aggrResult.get(groupByField);
                aggrResult.replace(groupByField, count+1);
            }
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

        if (no_grouping) {
            Type[] types = new Type[1];
            types[0] = Type.INT_TYPE;
            TupleDesc td = new TupleDesc(types);

            ArrayList<Tuple> list = new ArrayList<>();
            Tuple t = new Tuple(td);
            IntField field = new IntField(aggrResult2);
            t.setField(0, field);
            list.add(t);

            Iterable<Tuple> iterable = new Iterable<Tuple>() {
                @Override
                public Iterator<Tuple> iterator() {
                    return list.iterator();
                }
            };
            return new TupleIterator(td, iterable);

        } else {
            Type[] types = new Type[2];
            types[0] = groupByType;
            types[1] = Type.INT_TYPE;
            TupleDesc td = new TupleDesc(types);

            // add tuples to list
            ArrayList<Tuple> list = new ArrayList();
            for (Field key: aggrResult.keySet()) {
                int val = aggrResult.get(key);
                IntField value = new IntField(val);

                Tuple t = new Tuple(td);
                t.setField(0, key);
                t.setField(1, value);
                list.add(t);
            }

            // new a iterable
            Iterable<Tuple> iterable = new Iterable<Tuple>() {
                @Override
                public Iterator<Tuple> iterator() {
                    return list.iterator();
                }
            };

            // return a TupleIterator
            return new TupleIterator(td, iterable);
        }
    }

}
