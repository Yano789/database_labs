package simpledb.execution;

import java.io.IOException;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;

    /**
     * Constructor.
     *
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
            throw new DbException("young, black, and rich");
        this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the constructor. It returns a
     * one field tuple containing the number of inserted records. Inserts should be passed through
     * BufferPool. An instances of BufferPool is available via Database.getBufferPool(). Note that
     * insert DOES NOT need check to see if a particular tuple is a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or null if called more
     *         than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        BufferPool bufferPool = Database.getBufferPool();
        int insertedCount = 0;
        while (child.hasNext()) {
            try {
                bufferPool.insertTuple(t, tableId, child.next());
                insertedCount++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (insertedCount > 0) {
            Tuple resultTuple = new Tuple(td);
            resultTuple.setField(0, new IntField(insertedCount));
            return resultTuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }
}
