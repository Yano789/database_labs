package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.List;
import java.util.Iterator;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    // Instance fields
    private final int numPages;
    private final Map<PageId, Page> pageCache = new ConcurrentHashMap<>();
    private final LockManager lockManager = new LockManager();

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
    }


    public static int getPageSize() {
        return pageSize;
    }


    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
        BufferPool.pageSize = pageSize;
    }


    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // Acquire lock before accessing the page
        // lockManager.acquireLock(tid, pid, perm);

        // Check if page is already in cache
        // if (pageCache.containsKey(pid)) {
        // return pageCache.get(pid);
        // }

        // // If cache is full, evict a page
        // if (pageCache.size() >= numPages) {
        // evictPage();
        // }

        // // Read page from disk
        // DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        // Page page = dbFile.readPage(pid);
        // pageCache.put(pid, page);

        // return page;
        // yufeng: susceptible to race conditions

        // Acquire lock
        switch (perm) {
            case READ_ONLY:
                lockManager.acquireShared(tid, pid);
                break;
            case READ_WRITE:
                lockManager.acquireExclusive(tid, pid);
                break;
        }

        // Check cache
        AtomicReference<DbException> dbException = new AtomicReference<>(null);
        AtomicReference<IllegalArgumentException> illegalArgumentException = new AtomicReference<>(null);
        Page page = pageCache.computeIfAbsent(pid, k -> {
            if (pageCache.size() >= numPages) {
                try {
                    evictPage();
                } catch (DbException e) {
                    dbException.set(e);
                    switch (perm) {
                        case READ_ONLY:
                            lockManager.releaseShared(tid, pid);
                            break;
                        case READ_WRITE:
                            lockManager.releaseExclusive(tid, pid);
                            break;
                    }
                    return null;
                }
            }
            try {
                return Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            } catch (IllegalArgumentException e) {
                illegalArgumentException.set(e);
                switch (perm) {
                    case READ_ONLY:
                        lockManager.releaseShared(tid, pid);
                        break;
                    case READ_WRITE:
                        lockManager.releaseExclusive(tid, pid);
                        break;
                }
                return null;
            }
        });
        if (dbException.get() != null)
            throw dbException.get();
        if (illegalArgumentException.get() != null)
            throw illegalArgumentException.get();
        return page;
    }

    public void releaseExclusive(TransactionId tid, PageId pid) {
        lockManager.releaseExclusive(tid, pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            for (Map.Entry<PageId, Page> entry : pageCache.entrySet()) {
                Page page = entry.getValue();
                if (page.isDirty() != null && page.isDirty().equals(tid)) {
                    try {
                        flushPage(entry.getKey());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            Iterator<Map.Entry<PageId, Page>> iterator = pageCache.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<PageId, Page> entry = iterator.next();
                Page page = entry.getValue();
                if (page.isDirty() != null && page.isDirty().equals(tid)) {
                    iterator.remove();
                }
            }
        }

        lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtiedPages = file.insertTuple(tid, t);


        // Mark all dirtied pages as dirty and update cache
        for (Page page : dirtiedPages) {
            page.markDirty(true, tid);
            pageCache.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtiedPages = file.deleteTuple(tid, t);


        // Mark all dirtied pages as dirty and update cache
        for (Page page : dirtiedPages) {
            page.markDirty(true, tid);
            pageCache.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageCache.get(pid);
        if (page == null) {
            return;
        }

        if (page.isDirty() != null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            dbFile.writePage(page);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId victimId = null;
        for (Map.Entry<PageId, Page> entry : pageCache.entrySet()) {
            if (entry.getValue().isDirty() == null) {
                victimId = entry.getKey();
                break;
            }
        }

        if (victimId == null) {
            throw new DbException("Cannot evict page: all pages in buffer pool are dirty (NO STEAL policy)");
        }

        pageCache.remove(victimId);
    }

}
