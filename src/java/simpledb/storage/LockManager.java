package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.HashSet;

/**
 * LockManager handles lock acquisition and release for pages in the BufferPool.
 * It supports shared (READ_ONLY) and exclusive (READ_WRITE) locks.
 */
public class LockManager {

    /** Lock information for a page */
    private static class PageLock {
        private final Set<TransactionId> sharedLocks;
        private TransactionId exclusiveLock;
        
        public PageLock() {
            this.sharedLocks = new HashSet<>();
            this.exclusiveLock = null;
        }
        
        public synchronized boolean hasSharedLock(TransactionId tid) {
            return sharedLocks.contains(tid);
        }
        
        public synchronized boolean hasExclusiveLock(TransactionId tid) {
            return exclusiveLock != null && exclusiveLock.equals(tid);
        }
        
        public synchronized boolean hasAnyLock(TransactionId tid) {
            return hasSharedLock(tid) || hasExclusiveLock(tid);
        }
        
        public synchronized boolean canAcquireSharedLock(TransactionId tid) {
            // Can acquire shared lock if:
            // 1. No exclusive lock exists, OR
            // 2. The requesting transaction already holds the exclusive lock
            return exclusiveLock == null || exclusiveLock.equals(tid);
        }
        
        public synchronized boolean canAcquireExclusiveLock(TransactionId tid) {
            // Can acquire exclusive lock if:
            // 1. No locks exist, OR
            // 2. Only the requesting transaction holds a shared lock, OR
            // 3. The requesting transaction already holds the exclusive lock
            if (exclusiveLock != null) {
                return exclusiveLock.equals(tid);
            }
            return sharedLocks.isEmpty() || (sharedLocks.size() == 1 && sharedLocks.contains(tid));
        }
        
        public synchronized void acquireSharedLock(TransactionId tid) {
            if (!canAcquireSharedLock(tid)) {
                throw new IllegalStateException("Cannot acquire shared lock");
            }
            sharedLocks.add(tid);
        }
        
        public synchronized void acquireExclusiveLock(TransactionId tid) {
            if (!canAcquireExclusiveLock(tid)) {
                throw new IllegalStateException("Cannot acquire exclusive lock");
            }
            // If upgrading from shared to exclusive, remove from shared locks
            sharedLocks.remove(tid);
            exclusiveLock = tid;
        }
        
        public synchronized void releaseLock(TransactionId tid) {
            sharedLocks.remove(tid);
            if (exclusiveLock != null && exclusiveLock.equals(tid)) {
                exclusiveLock = null;
            }
        }
        
        public synchronized void releaseAllLocks(TransactionId tid) {
            sharedLocks.remove(tid);
            if (exclusiveLock != null && exclusiveLock.equals(tid)) {
                exclusiveLock = null;
            }
        }
        
        public synchronized boolean isEmpty() {
            return sharedLocks.isEmpty() && exclusiveLock == null;
        }
    }

    // Map from PageId to PageLock
    private final ConcurrentHashMap<PageId, PageLock> pageLocks;
    
    // Map from TransactionId to set of PageIds it holds locks on
    private final ConcurrentHashMap<TransactionId, Set<PageId>> transactionLocks;

    public LockManager() {
        this.pageLocks = new ConcurrentHashMap<>();
        this.transactionLocks = new ConcurrentHashMap<>();
    }

    /**
     * Acquire a lock on the given page for the given transaction.
     * This method will block until the lock can be acquired.
     */
    public void acquireLock(TransactionId tid, PageId pageId, Permissions perm) {
        while (true) {
            PageLock pageLock = pageLocks.computeIfAbsent(pageId, k -> new PageLock());
            
            synchronized (pageLock) {
                if (perm == Permissions.READ_ONLY) {
                    if (pageLock.canAcquireSharedLock(tid)) {
                        pageLock.acquireSharedLock(tid);
                        addTransactionLock(tid, pageId);
                        return;
                    }
                } else { // READ_WRITE
                    if (pageLock.canAcquireExclusiveLock(tid)) {
                        pageLock.acquireExclusiveLock(tid);
                        addTransactionLock(tid, pageId);
                        return;
                    }
                }
            }
            
            // Wait and retry
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for lock", e);
            }
        }
    }

    /**
     * Release a specific lock held by a transaction on a page.
     */
    public void releaseLock(TransactionId tid, PageId pageId) {
        PageLock pageLock = pageLocks.get(pageId);
        if (pageLock != null) {
            synchronized (pageLock) {
                pageLock.releaseLock(tid);
                removeTransactionLock(tid, pageId);
                
                // Clean up empty lock entries
                if (pageLock.isEmpty()) {
                    pageLocks.remove(pageId);
                }
            }
        }
    }

    /**
     * Release all locks held by a transaction.
     */
    public void releaseAllLocks(TransactionId tid) {
        Set<PageId> pages = transactionLocks.remove(tid);
        if (pages != null) {
            for (PageId pageId : pages) {
                PageLock pageLock = pageLocks.get(pageId);
                if (pageLock != null) {
                    synchronized (pageLock) {
                        pageLock.releaseAllLocks(tid);
                        
                        // Clean up empty lock entries
                        if (pageLock.isEmpty()) {
                            pageLocks.remove(pageId);
                        }
                    }
                }
            }
        }
    }

    /**
     * Check if a transaction holds any lock on the given page.
     */
    public boolean holdsLock(TransactionId tid, PageId pageId) {
        PageLock pageLock = pageLocks.get(pageId);
        if (pageLock == null) {
            return false;
        }
        synchronized (pageLock) {
            return pageLock.hasAnyLock(tid);
        }
    }

    /**
     * Add a page to the set of pages locked by a transaction.
     */
    private void addTransactionLock(TransactionId tid, PageId pageId) {
        transactionLocks.computeIfAbsent(tid, k -> new HashSet<>()).add(pageId);
    }

    /**
     * Remove a page from the set of pages locked by a transaction.
     */
    private void removeTransactionLock(TransactionId tid, PageId pageId) {
        Set<PageId> pages = transactionLocks.get(tid);
        if (pages != null) {
            pages.remove(pageId);
            if (pages.isEmpty()) {
                transactionLocks.remove(tid);
            }
        }
    }
}
