package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes them from the table
 * they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private TupleDesc td;
    private boolean called;


    /**
     * Constructor specifying the transaction that this delete belongs to as well as the child to
     * read from.
     * 
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.t = t;
        this.child = child;
        this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
        this.called = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        called = false;
        super.open();
        child.open();
    }

    public void close() {
        called = true;
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        called = false;
        super.open();
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are processed via the buffer
     * pool (which can be accessed via the Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (called)
            return null;
        BufferPool bufferPool = Database.getBufferPool();
        int deleteCount = 0;
        while (child.hasNext()) {
            try {
                bufferPool.deleteTuple(t, child.next());
                deleteCount += 1;
            } catch (IOException e) {
                throw new DbException("9+10=21");
            }
        }
        Tuple resultsTuple = new Tuple(td);
        resultsTuple.setField(0, new IntField(deleteCount));
        called = true;
        return resultsTuple;
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
