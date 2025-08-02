package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * LockManager handles lock acquisition and release for pages in the BufferPool.
 * It supports shared (READ_ONLY) and exclusive (READ_WRITE) locks.
 */
public class LockManager {

    // Map from PageId to PageLock
    private final ConcurrentHashMap<PageId, PageLock> pageLocks;

    // Map from TransactionId to set of PageIds it holds locks on
    private final ConcurrentHashMap<TransactionId, Set<PageId>> transactionLocks;
    private final WaitForGraph wfg;

    public LockManager() {
        this.pageLocks = new ConcurrentHashMap<>();
        this.transactionLocks = new ConcurrentHashMap<>();
        this.wfg = new WaitForGraph();
    }

    /**
     * Acquire a lock on the given page for the given transaction.
     * This method will block until the lock can be acquired.
     */
    // public void acquireLock(TransactionId tid, PageId pageId, Permissions perm) {
    // while (true) {
    // PageLock pageLock = pageLocks.computeIfAbsent(pageId, k -> new PageLock());

    // synchronized (pageLock) {
    // if (perm == Permissions.READ_ONLY) {
    // if (pageLock.canAcquireSharedLock(tid)) {
    // pageLock.acquireSharedLock(tid);
    // addTransactionLock(tid, pageId);
    // return;
    // }
    // } else { // READ_WRITE
    // if (pageLock.canAcquireExclusiveLock(tid)) {
    // pageLock.acquireExclusiveLock(tid);
    // addTransactionLock(tid, pageId);
    // return;
    // }
    // }
    // }

    // // Wait and retry
    // try {
    // Thread.sleep(10);
    // } catch (InterruptedException e) {
    // Thread.currentThread().interrupt();
    // throw new RuntimeException("Interrupted while waiting for lock", e);
    // }
    // }
    // }
    // yufeng: ex5 requires actual blocking

    public void acquireShared(TransactionId transactionId, PageId pageId) throws TransactionAbortedException {
        try {
            // Sync on pageLock
            PageLock pageLock = pageLocks.computeIfAbsent(pageId, k -> new PageLock());
            pageLock.synchronize();

            // Add edge if need to wait
            TransactionId transactionToBeWaitedOn = null;
            if (!pageLock.canAcquireSharedLock(transactionId)) {
                transactionToBeWaitedOn = pageLock.getLastWaitingTransactionId();
                if (transactionToBeWaitedOn == null)
                    transactionToBeWaitedOn = pageLock.getExclusiveTransactionId();
                wfg.incrementWait(transactionId, transactionToBeWaitedOn);
                if (wfg.hasCycle()) {
                    wfg.removeNode(transactionId);
                    pageLock.desynchronize();
                    throw new TransactionAbortedException();
                }
            }

            // Acquire shared lock
            pageLock.acquireSharedLock(transactionId);

            // Remove edge if present
            if (transactionToBeWaitedOn != null) {
                final TransactionId finalTransactionToBeWaitedOn = transactionToBeWaitedOn;
                wfg.decrementWait(transactionId, finalTransactionToBeWaitedOn);
            }

            // Desync on pageLock
            pageLock.desynchronize();

            // Add to lockedPages
            transactionLocks.computeIfAbsent(transactionId, k -> ConcurrentHashMap.newKeySet())
                    .add(pageId);
        } catch (InterruptedException e) {
        }
    }

    public void acquireExclusive(TransactionId transactionId, PageId pageId) throws TransactionAbortedException {
        try {
            // Sync on pageLock
            PageLock pageLock = pageLocks.computeIfAbsent(pageId, k -> new PageLock());
            pageLock.synchronize();

            // Add edge if need to wait
            Set<TransactionId> transactionsToBeWaitedOn = null;
            if (!pageLock.canAcquireExclusiveLock(transactionId)) {
                transactionsToBeWaitedOn = getTransactionsToBeWaitedOnIfCannotAcquireExclusive(pageLock);
                for (TransactionId transactionToBeWaitedOn : transactionsToBeWaitedOn) {
                    wfg.incrementWait(transactionId, transactionToBeWaitedOn);
                    if (wfg.hasCycle()) {
                        wfg.removeNode(transactionId);
                        pageLock.desynchronize();
                        throw new TransactionAbortedException();
                    }
                }
            }

            // Acquire exclusive lock
            pageLock.acquireExclusiveLock(transactionId);

            // Remove edge if present
            if (transactionsToBeWaitedOn != null)
                for (TransactionId transactionToBeWaitedOn : transactionsToBeWaitedOn)
                    wfg.decrementWait(transactionId, transactionToBeWaitedOn);

            // Desync on pageLock
            pageLock.desynchronize();

            // Add to lockedPages
            transactionLocks.computeIfAbsent(transactionId, k -> ConcurrentHashMap.newKeySet())
                    .add(pageId);
        } catch (InterruptedException e) {
        }
    }

    private Set<TransactionId> getTransactionsToBeWaitedOnIfCannotAcquireExclusive(PageLock pageLock) {
        Set<TransactionId> transactionsToBeWaitedOn = new HashSet<>();
        TransactionId lastTransactionId = pageLock.getLastWaitingTransactionId();
        if (lastTransactionId == null) {
            switch (pageLock.getPageLockState()) {
                case EXCLUSIVE:
                    transactionsToBeWaitedOn.add(pageLock.getExclusiveTransactionId());
                    break;
                case SHARED:
                    Iterator<TransactionId> iterator = pageLock.getSharedTransactionIds().iterator();
                    while (iterator.hasNext())
                        transactionsToBeWaitedOn.add(iterator.next());
                    break;
                default:
                    System.err.println("Unknown lock state");
                    break;
            }
        } else {
            transactionsToBeWaitedOn.add(lastTransactionId);
        }
        return transactionsToBeWaitedOn;
    }

    /**
     * Release a specific lock held by a transaction on a page.
     */
    public void releaseLock(TransactionId tid, PageId pageId) {
        PageLock pageLock = pageLocks.get(pageId);
        if (pageLock != null) {
            pageLock.synchronize();
            pageLock.releaseAllLocks(tid);

            // Clean up empty lock entries
            // if (pageLock.isEmpty()) {
            // pageLocks.remove(pageId);
            // }
            // yufeng: not thread safe replaced with below
            pageLocks.computeIfPresent(pageId, (k, v) -> {
                if (v.isEmpty())
                    return null;
                return v;
            });
            pageLock.desynchronize();
        }
    }
    // yufeng: need to be able to release shared or specific

    public void releaseShared(TransactionId transactionId, PageId pageId) {
        PageLock pageLock = pageLocks.get(pageId);
        pageLock.synchronize();
        if (pageLock != null) {
            pageLock.releaseShared(transactionId);
            if (!pageLock.hasAnyLock(transactionId)) {
                transactionLocks.compute(transactionId, (k, v) -> {
                    v.remove(pageId);
                    if (v.isEmpty())
                        return null;
                    return v;
                });
            }
        }
        pageLock.desynchronize();
    }

    public void releaseExclusive(TransactionId transactionId, PageId pageId) {
        PageLock pageLock = pageLocks.get(pageId);
        pageLock.synchronize();
        if (pageLock != null) {
            pageLock.releaseExclusive(transactionId);
            if (!pageLock.hasAnyLock(transactionId)) {
                transactionLocks.compute(transactionId, (k, v) -> {
                    v.remove(pageId);
                    if (v.isEmpty())
                        return null;
                    return v;
                });
            }
        }
        pageLock.desynchronize();
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
                    pageLock.synchronize();
                    pageLock.releaseAllLocks(tid);

                    // Clean up empty lock entries
                    if (pageLock.isEmpty()) {
                        pageLocks.remove(pageId);
                    }
                    pageLock.desynchronize();
                }
            }
        }
        // transactionLocks.computeIfPresent(tid, (k, v) -> {
        // for (PageId pageId : v) {
        // pageLocks.computeIfPresent(pageId, (kk, vv) -> {
        // vv.synchronize();
        // vv.releaseAllLocks(tid);
        // vv.desynchronize();
        // return vv;
        // });
        // }
        // return null;
        // });

    }

    /**
     * Check if a transaction holds any lock on the given page.
     */
    public boolean holdsLock(TransactionId tid, PageId pageId) {
        PageLock pageLock = pageLocks.get(pageId);
        if (pageLock == null) {
            return false;
        }
        pageLock.synchronize();
        boolean result = pageLock.hasAnyLock(tid);
        pageLock.desynchronize();
        return result;
    }

    public Set<PageId> getPagesLockedByTransaction(TransactionId transactionId) {
        return transactionLocks.get(transactionId);
    }

}
