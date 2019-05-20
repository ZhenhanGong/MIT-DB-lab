package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int groupByFieldIndex;
    private Type groupByType;
    private int fieldIndex;
    private Op op;

    // grouping
    private HashMap<Field, Double> aggrResult;
    private HashMap<Field, Integer> cnt;

    // no grpuping
    private boolean no_grouping;
    private double aggrResult2;
    private int cnt2;

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
        if (gbfield == Aggregator.NO_GROUPING) {
            no_grouping = true;
        }
        groupByFieldIndex = gbfield;
        groupByType = gbfieldtype;
        fieldIndex = afield;
        op = what;
        aggrResult = new HashMap<>();
        cnt = new HashMap<>();
        aggrResult2 = Double.MIN_VALUE;
        cnt2 = 0;
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

        // get value
        IntField valueField = (IntField) tup.getField(fieldIndex);
        int value = valueField.getValue();

        if (no_grouping) {
            switch (op) {
                case MIN:
                    if (aggrResult2 == Integer.MIN_VALUE)
                        aggrResult2 = value;
                    if (value < aggrResult2)
                        aggrResult2 = value;
                    break;
                case MAX:
                    if (aggrResult2 == Integer.MIN_VALUE)
                        aggrResult2 = value;
                    if (value > aggrResult2)
                        aggrResult2 = value;
                    break;
                case SUM:
                    if (aggrResult2 == Integer.MIN_VALUE)
                        aggrResult2 = 0;
                    aggrResult2 += value;
                    break;
                case AVG:
                    cnt2++;
                    aggrResult2 = (aggrResult2 * (cnt2-1) + value) / cnt2;
                    break;
                case COUNT:
                    cnt2++;
                    aggrResult2 = cnt2;
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

        } else {
            Field groupByField =  tup.getField(groupByFieldIndex);

            // need to create a new group
            if (!aggrResult.containsKey(groupByField)) {
                if (op == Op.COUNT)
                    aggrResult.put(groupByField, 1.0);
                else
                    aggrResult.put(groupByField, (double)value);
                cnt.put(groupByField, 1);

            } else {
                double oldValue = aggrResult.get(groupByField);
                int count = cnt.get(groupByField);
                cnt.replace(groupByField, count + 1);

                switch (op) {
                    case MIN:
                        if (value < oldValue)
                            aggrResult.replace(groupByField, (double)value);
                        break;
                    case MAX:
                        if (value > oldValue)
                            aggrResult.replace(groupByField, (double)value);
                        break;
                    case SUM:
                        aggrResult.replace(groupByField, value + oldValue);
                        break;
                    case AVG:
                        double avg = aggrResult.get(groupByField);
                        double result = (count * avg + value) / (count + 1);
                        aggrResult.replace(groupByField, result);
                        break;
                    case COUNT:
                        aggrResult.replace(groupByField, (double) (count+1));
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
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

        if (no_grouping) {
            Type[] types = new Type[1];
            types[0] = Type.INT_TYPE;
            TupleDesc td = new TupleDesc(types);

            ArrayList<Tuple> list = new ArrayList<>();
            Tuple t = new Tuple(td);
            IntField field = new IntField((int) aggrResult2);
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
            // new a tuple descriptor
            Type[] types = new Type[2];
            types[0] = groupByType;
            types[1] = Type.INT_TYPE;
            TupleDesc td = new TupleDesc(types);

            // add tuples to list
            ArrayList<Tuple> list = new ArrayList();
            for (Field key: aggrResult.keySet()) {
                double val = aggrResult.get(key);
                IntField value = new IntField((int)val);

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
