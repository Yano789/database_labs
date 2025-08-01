package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples in no particular
 * order. Tuples are stored on pages, each of which is a fixed size, and the file is simply a
 * collection of those pages. HeapFile works closely with HeapPage. The format of HeapPages is
 * described in the HeapPage constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;
    private final int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        this.tableId = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note: you will need to
     * generate this tableid somewhere to ensure that each HeapFile has a "unique id," and that you
     * always return the same value for a particular HeapFile. We suggest hashing the absolute file
     * name of the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        int pageNumber = pid.getPageNumber();
        long offset = (long) pageNumber * pageSize;
        byte[] data = new byte[pageSize];

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(offset);
            raf.readFully(data);
            raf.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read page " + pid, e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pageSize = BufferPool.getPageSize();
        int pageNumber = page.getId().getPageNumber();
        long offset = (long) pageNumber * pageSize;
        byte[] data = page.getPageData();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.seek(offset);
        raf.write(data);
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> results = new ArrayList<>();
        HeapPage page;
        int numPages = numPages();
        BufferPool bufferPool = Database.getBufferPool();
        for (int i = 0; i < numPages; i++) {
            page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(tableId, i),
                    Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                results.add(page);
                break;
            }
        }
        if (results.isEmpty()) {
            writePage(new HeapPage(new HeapPageId(tableId, numPages),
                    HeapPage.createEmptyPageData()));
            page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(tableId, numPages),
                    Permissions.READ_WRITE);
            page.insertTuple(t);
            results.add(page);
        }
        return results;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        ArrayList<Page> results = new ArrayList<>();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,
                t.getRecordId().getPageId(), Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        results.add(heapPage);
        return results;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    private static class HeapFileIterator implements DbFileIterator {
        private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> tupleIterator;
        private int currentPage;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            currentPage = 0;
            tupleIterator = getPageTuples(currentPage);
        }

        private Iterator<Tuple> getPageTuples(int pageNumber)
                throws TransactionAbortedException, DbException {
            if (pageNumber >= 0 && pageNumber < heapFile.numPages()) {
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNumber);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid,
                        Permissions.READ_ONLY);
                return page.iterator();
            }
            return null;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null) {
                return false;
            }

            while (!tupleIterator.hasNext()) {
                currentPage++;
                if (currentPage >= heapFile.numPages()) {
                    return false;
                }
                tupleIterator = getPageTuples(currentPage);
            }
            return true;
        }

        @Override
        public Tuple next()
                throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tupleIterator == null || !hasNext()) {
                throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupleIterator = null;
        }
    }

}

