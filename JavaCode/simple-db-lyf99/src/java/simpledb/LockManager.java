package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private Map<PageId, HashSet<TransactionId>> sharedLocks;
    private Map<PageId, TransactionId> exclusiveLocks;
    private Map<TransactionId, HashSet<TransactionId>> graph;


    public LockManager() {
        sharedLocks = new ConcurrentHashMap<>();
        exclusiveLocks = new ConcurrentHashMap<>();
        graph = new ConcurrentHashMap<>();
    }

    public synchronized void grabLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        // if the lock is held, we just simply return
        if (perm.equals(Permissions.READ_ONLY)) {  // READ_ONLY
            if (holdsLock(tid, pid)) {
                return;
            }
            create_sharedLocks(tid, pid, perm);
        } else {  // READ_WRITE
            if (holdsExclusiveLock(tid, pid)) {
                // tid already has an exclusive lock with this tid on this page
                return;
            }
            create_exclusiveLocks(tid, pid, perm);
        }
    }

    private void addToGraph(TransactionId tid, PageId pid) throws TransactionAbortedException {
        HashSet<TransactionId> wait = (graph.containsKey(tid)) ? graph.get(tid) : new HashSet<>();
        wait.add(exclusiveLocks.get(pid));
        graph.put(tid, wait);
        if (detectDeadLock(tid)) {
            removeFromGraph(tid);
            throw new TransactionAbortedException();
        }
    }

    // private helper method for waiting the exclusive/shared lock recursively
    // by using recursive grabLock function
    // @Parameters: TransactionId tid, PageId pid, Permissions perm
    private void waitLock(TransactionId tid, PageId pid, Permissions perm) {
        try {
            this.wait();
            grabLock(tid, pid, perm);
        } catch (InterruptedException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    // private void create sharedLocks, put the pid and tid in the sharedLocks(map)
    private void create_sharedLocks(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if (exclusiveLocks.containsKey(pid)) {
            addToGraph(tid, pid);
            waitLock(tid, pid, perm);
        }
        // no exclusive lock on this page
        HashSet<TransactionId> tids = (sharedLocks.containsKey(pid)) ? sharedLocks.get(pid) : new HashSet<TransactionId>();
        tids.add(tid);
        sharedLocks.put(pid, tids);
        removeFromGraph(tid);
    }

    // private void create exclusiveLocks, put the pid and tid in the exclusiveLocks(map)
    private void create_exclusiveLocks(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if (exclusiveLocks.containsKey(pid)) {
            addToGraph(tid, pid);
            waitLock(tid,pid,perm);
        } else {
            if (sharedLocks.containsKey(pid)) {
                HashSet<TransactionId> tids = sharedLocks.get(pid);
                if (tids.size() == 1 && tids.contains(tid)) {
                    // when there is a shared lock on this tid, we need upgrade to an exclusive lock
                    sharedLocks.remove(pid);
                    exclusiveLocks.put(pid, tid);
                    removeFromGraph(tid);
                } else {
                    HashSet<TransactionId> wait = graph.containsKey(tid) ? graph.get(tid) : new HashSet<>();
                    wait.addAll(tids);
                    wait.remove(tid);
                    graph.put(tid, wait);
                    if (detectDeadLock(tid)) {
                        removeFromGraph(tid);
                        throw new TransactionAbortedException();
                    }
                    waitLock(tid, pid, perm);
                }
            } else {  // there is no lock on this page
                exclusiveLocks.put(pid, tid);
                removeFromGraph(tid);
            }
        }
    }

    // releaseLockTid release the lock with tid only
    public synchronized void releaseLockTid(TransactionId tid) {
        Set<PageId> releasePages = new HashSet<>();
        for (PageId pid : sharedLocks.keySet()) {
            HashSet<TransactionId> tids = sharedLocks.get(pid);
            if (tids != null && tids.contains(tid)) {
                tids.remove(tid);
                if (tids.size() == 0) {
                    releasePages.add(pid);
                } else {
                    sharedLocks.put(pid, tids);
                }
            }
        }
        for (PageId pid : releasePages) {
            sharedLocks.remove(pid);
        }
        releasePages.clear();

        for (PageId pid : exclusiveLocks.keySet()) {
            if (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) {
                releasePages.add(pid);
            }
        }
        for (PageId pid : releasePages) {
            exclusiveLocks.remove(pid);
        }
        notifyAll();
    }

    // release Lock release the Lock, and need two parameters: tid, and pid
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        HashSet<TransactionId> tids = sharedLocks.get(pid);
        if (tids != null && tids.contains(tid)) {
            tids.remove(tid);
            if (tids.size() == 0) {
                sharedLocks.remove(pid);
            } else {
                sharedLocks.put(pid, tids);
            }
        }
        if (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) {
            exclusiveLocks.remove(pid);
        }
        notifyAll();
    }

    // returns true if the transaction has a lock on the certain page
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return holdsSharedLock(tid, pid) || holdsExclusiveLock(tid, pid);
    }

    private boolean holdsSharedLock(TransactionId tid, PageId pid) {
        return sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid);
    }

    private boolean holdsExclusiveLock(TransactionId tid, PageId pid) {
        return exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid);
    }

    private synchronized boolean detectDeadLock(TransactionId tid) {
        Set<TransactionId> attended = new HashSet<>();
        Queue<TransactionId> q = new LinkedList<>();
        q.add(tid);
        while(!q.isEmpty()) {
            TransactionId nextNode = q.remove();
            attended.add(nextNode);
            if (graph.containsKey(nextNode)) {
                HashSet<TransactionId> waitingSet = graph.get(nextNode);
                for (TransactionId waitTid : waitingSet) {
                    if (!attended.contains(waitTid)) {
                        q.add(waitTid);
                    } else {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private synchronized void removeFromGraph(TransactionId tid) {
        graph.remove(tid);
        for (TransactionId key : graph.keySet()) {
            graph.get(key).remove(tid);
        }
    }
}
