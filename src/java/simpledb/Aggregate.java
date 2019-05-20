package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private boolean no_grouping;
    private OpIterator child;
    private int fieldIndex;
    private int groupFieldIndex;
    private Aggregator.Op op;
    private Aggregator aggregator;
    private OpIterator aggrIterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        if (gfield == Aggregator.NO_GROUPING)
            no_grouping = true;
        this.child = child;
        fieldIndex = afield;
        groupFieldIndex = gfield;
        op = aop;

        Type type = child.getTupleDesc().getFieldType(afield);
        if (no_grouping) {
            if (type == Type.INT_TYPE) {
                aggregator = new IntegerAggregator(Aggregator.NO_GROUPING, null, afield, aop);
            } else if (type == Type.STRING_TYPE) {
                aggregator = new StringAggregator(Aggregator.NO_GROUPING, null, afield, aop);
            }
        } else {
            Type gType = child.getTupleDesc().getFieldType(gfield);
            if (type == Type.INT_TYPE) {
                aggregator = new IntegerAggregator(gfield, gType, afield, aop);
            } else if (type == Type.STRING_TYPE) {
                aggregator = new StringAggregator(gfield, gType, afield, aop);
            }
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
        if (no_grouping)
            return Aggregator.NO_GROUPING;
        else
            return groupFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if (no_grouping)
            return null;
        else
            return child.getTupleDesc().getFieldName(groupFieldIndex);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
        return fieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
        return child.getTupleDesc().getFieldName(fieldIndex);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        child.open();
        super.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        aggrIterator = aggregator.iterator();
        aggrIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if (aggrIterator.hasNext())
            return aggrIterator.next();
        else
            return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        child.rewind();
        aggrIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
        return child.getTupleDesc();
    }

    public void close() {
	// some code goes here
        super.close();
        child.close();
        aggrIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
        return new OpIterator[]{ this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        if (this.child != children[0])
            this.child = children[0];
    }
    
}
