package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;

    // fetchNext() return # of inserted tuples only once
    private boolean resReturned;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        td = Database.getCatalog().getTupleDesc(tableId);
        if (!child.getTupleDesc().equals(td))
            throw new DbException("tuple desc not match");

        tid = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        resReturned = false;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        resReturned = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!resReturned) {
            int cnt = 0;
            while (child.hasNext()) {
                Tuple tuple = child.next();
                try {
                    Database.getBufferPool().insertTuple(tid, tableId, tuple);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                cnt++;
            }

            Type types[] = new Type[1];
            types[0] = Type.INT_TYPE;

            TupleDesc tupleDesc = new TupleDesc(types);
            IntField field = new IntField(cnt);

            Tuple t = new Tuple(tupleDesc);
            t.setField(0, field);
            resReturned = true;
            return t;
        } else
            return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (this.child != children[0])
            this.child = children[0];
    }
}
