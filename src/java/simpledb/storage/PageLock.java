package simpledb.storage;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import simpledb.transaction.TransactionId;

/**
 * To be accessed by only {@link LockManager}. {@link LockManager} is needed to
 * synchronize manually using {@link PageLock#synchronize} and
 * {@link PageLock#desynchronize} methods before calling
 * {@link PageLock#acquireExclusive}, {@link PageLock#acquireSharedLock},
 * {@link PageLock#releaseAllLocks}
 * {@link PageLock#releaseExclusive}, {@link PageLock#releaseShared}. This is
 * for optimization purposes.
 */
public class PageLock {
    private final Set<TransactionId> sharedLocks;
    private TransactionId exclusiveLock;
    private PageLockState pageLockState;
    private final Queue<Waiter> waiters;
    private TransactionId lastWaitingTransactionId;
    private final ReentrantLock reentrantLock;
    private final Condition condition;
    private TransactionId nextTransactionId;

    public PageLock() {
        this.sharedLocks = new HashSet<>();
        this.exclusiveLock = null;
        this.pageLockState = PageLockState.NONE;
        this.waiters = new LinkedList<>();
        this.lastWaitingTransactionId = null;
        this.reentrantLock = new ReentrantLock(true);
        this.condition = reentrantLock.newCondition();
        this.nextTransactionId = null;
    }

    public boolean hasSharedLock(TransactionId tid) {
        return sharedLocks.contains(tid);
    }

    public boolean hasExclusiveLock(TransactionId tid) {
        return exclusiveLock != null && exclusiveLock.equals(tid);
    }

    public boolean hasAnyLock(TransactionId tid) {
        return hasSharedLock(tid) || hasExclusiveLock(tid);
    }

    public boolean canAcquireSharedLock(TransactionId tid) {
        // Can acquire shared lock if:
        // 1. No exclusive lock exists, OR
        // 2. The requesting transaction already holds the exclusive lock
        // return exclusiveLock == null || exclusiveLock.equals(tid);
        // yufeng: kept above logic but took into account new properties
        if (!waiters.isEmpty())
            return false;
        switch (pageLockState) {
            case NONE:
                return true;
            case SHARED:
                return true;
            case EXCLUSIVE:
                return exclusiveLock.equals(tid);
            default:
                System.err.println("Unknown transaction lock state");
                return false;
        }
    }

    public boolean canAcquireExclusiveLock(TransactionId tid) {
        // Can acquire exclusive lock if:
        // 1. No locks exist, OR
        // 2. Only the requesting transaction holds a shared lock, OR
        // 3. The requesting transaction already holds the exclusive lock
        // if (exclusiveLock != null) {
        // return exclusiveLock.equals(tid);
        // }
        // return sharedLocks.isEmpty() || (sharedLocks.size() == 1 &&
        // sharedLocks.contains(tid));
        // yufeng: kept above logic but took into account new properties
        if (!waiters.isEmpty())
            return false;
        switch (pageLockState) {
            case NONE:
                return true;
            case SHARED:
                return sharedLocks.size() <= 1 && sharedLocks.contains(tid);
            case EXCLUSIVE:
                return exclusiveLock.equals(tid);
            default:
                System.err.println("Unknown transaction lock state");
                return false;
        }
    }

    public void acquireSharedLock(TransactionId tid) throws InterruptedException {
        // if (!canAcquireSharedLock(tid)) {
        // throw new IllegalStateException("Cannot acquire shared lock");
        // }
        // sharedLocks.add(tid);
        // yufeng: ex5 requires blocking
        if (!waiters.isEmpty())
            wait(tid, WaiterType.SHARED);
        while (true) {
            switch (pageLockState) {
                case NONE:
                    pageLockState = PageLockState.SHARED;
                    sharedLocks.add(tid);
                    return;
                case SHARED:
                    sharedLocks.add(tid);
                    return;
                case EXCLUSIVE:
                    if (exclusiveLock.equals(tid)) {
                        sharedLocks.add(tid);
                        return;
                    }
                    wait(tid, WaiterType.SHARED);
                    break;
            }
        }
    }

    public void acquireExclusiveLock(TransactionId tid) throws InterruptedException {
        // if (!canAcquireExclusiveLock(tid)) {
        // throw new IllegalStateException("Cannot acquire exclusive lock");
        // }
        // // If upgrading from shared to exclusive, remove from shared locks
        // sharedLocks.remove(tid);
        // exclusiveLock = tid;
        // yufeng: ex5 requires that the shared lock be kept
        if (!waiters.isEmpty())
            wait(tid, WaiterType.EXCLUSIVE);
        while (true) {
            switch (pageLockState) {
                case NONE:
                    pageLockState = PageLockState.EXCLUSIVE;
                    exclusiveLock = tid;
                    return;
                case SHARED:
                    if (sharedLocks.size() == 1
                            && sharedLocks.contains(tid)) {
                        pageLockState = PageLockState.EXCLUSIVE;
                        exclusiveLock = tid;
                        return;
                    }
                    wait(tid, WaiterType.EXCLUSIVE);
                    break;
                case EXCLUSIVE:
                    if (exclusiveLock.equals(tid))
                        return;
                    wait(tid, WaiterType.EXCLUSIVE);
                    break;
            }
        }
    }

    private void wait(TransactionId transactionId, WaiterType waiterType) throws InterruptedException {
        lastWaitingTransactionId = transactionId;
        waiters.add(new Waiter(transactionId, waiterType));
        while (!transactionId.equals(nextTransactionId))
            condition.await();
    }

    public void releaseShared(TransactionId transactionId) {
        switch (pageLockState) {
            case SHARED:
                sharedLocks.remove(transactionId);
                if (sharedLocks.isEmpty()) {
                    pageLockState = PageLockState.NONE;
                    pollQueueAndSignalOne();
                } else if (sharedLocks.size() == 1 && !waiters.isEmpty()
                        && sharedLocks.contains(waiters.peek().getTransactionId())) {
                    // First waiter cannot be a waiter acquiring shared lock as it would have
                    // already acquired and not wait if so
                    while (true) {
                        // Only waiters with the same transactionId can be woken
                        // subsequently as the first waiter would have upgraded to exclusive lock
                        pollQueueAndSignalOne();
                        Waiter waiter = waiters.peek();
                        if (waiter == null || !sharedLocks.contains(waiter.getTransactionId()))
                            return;
                    }
                }
                return;
            case EXCLUSIVE:
                sharedLocks.remove(transactionId);
                return;
            case NONE:
                return;
        }
    }

    public void releaseExclusive(TransactionId transactionId) {
        switch (pageLockState) {
            case SHARED:
                return;
            case EXCLUSIVE:
                if (exclusiveLock.equals(transactionId)) {
                    exclusiveLock = null;
                    if (sharedLocks.contains(transactionId)) {
                        pageLockState = PageLockState.SHARED;
                        while (waiters.peek() != null && waiters.peek().getWaiterType().equals(WaiterType.SHARED))
                            pollQueueAndSignalOne();
                    } else {
                        pageLockState = PageLockState.NONE;
                        pollQueueAndSignalOne();
                    }
                }
                return;
            case NONE:
                return;
        }
    }

    private void pollQueueAndSignalOne() {
        Waiter polledWaiter = waiters.poll();
        if (polledWaiter != null) {
            nextTransactionId = polledWaiter.getTransactionId();
            if (waiters.isEmpty())
                lastWaitingTransactionId = null;
            condition.signal();
        }
    }

    public void releaseAllLocks(TransactionId tid) {
        // sharedLocks.remove(tid);
        // if (exclusiveLock != null && exclusiveLock.equals(tid)) {
        // exclusiveLock = null;
        // }
        // yufeng: need to take into account blocking
        releaseShared(tid);
        releaseExclusive(tid);
    }

    public boolean isEmpty() {
        return sharedLocks.isEmpty() && exclusiveLock == null;
    }

    public PageLockState getPageLockState() {
        return pageLockState;
    }

    public TransactionId getExclusiveTransactionId() {
        return exclusiveLock;
    }

    public Set<TransactionId> getSharedTransactionIds() {
        return sharedLocks;
    }

    public void synchronize() {
        this.reentrantLock.lock();
    }

    public void desynchronize() {
        this.reentrantLock.unlock();
    }

    public TransactionId getLastWaitingTransactionId() {
        return lastWaitingTransactionId;
    }

    public Set<TransactionId> getTransactionsThatAcquired() {
        Set<TransactionId> result = new HashSet<>();
        switch (pageLockState) {
            case NONE:
                return new HashSet<>();
            case SHARED:
                return sharedLocks;
            case EXCLUSIVE:
                result.add(exclusiveLock);
                return result;
        }
        return result;
    }

    public static enum PageLockState {
        NONE, SHARED, EXCLUSIVE
    }

    private class Waiter {
        private TransactionId transactionId;
        private WaiterType waiterType;

        private Waiter(TransactionId transactionId, WaiterType waiterType) {
            this.transactionId = transactionId;
            this.waiterType = waiterType;
        }

        public TransactionId getTransactionId() {
            return transactionId;
        }

        public WaiterType getWaiterType() {
            return waiterType;
        }
    }

    private static enum WaiterType {
        SHARED, EXCLUSIVE
    }
}