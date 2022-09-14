package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;



/**
 * @author jervisliao
 * @create 2022-08-23 19:16
 */
public class LockManager {
    private ConcurrentHashMap<PageId, Lock> pageMap;

    private static final long MAX_TIMEOUT = 1000;

    public LockManager() {
        pageMap = new ConcurrentHashMap<>();
    }


    public interface Lock {
    }

    static class SharedLock implements Lock {
        HashSet<TransactionId> readers;

        SharedLock(HashSet<TransactionId> readers) {
            this.readers = readers;
        }
    }

    static class ExclusiveLock implements Lock {
        TransactionId owner;

        ExclusiveLock(TransactionId owner) {
            this.owner = owner;
        }
    }


    public Lock getLock(PageId pid) {
        return pageMap.getOrDefault(pid, null);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        Lock lock = pageMap.get(pid);
        if (lock == null) return false;
        if (lock instanceof SharedLock) {
            return ((SharedLock) lock).readers.contains(tid);
        } else {
            return ((ExclusiveLock) lock).owner == tid;
        }
    }

    //其实我都已经在Database中注册过LockManager了，pageMap按理来说就是线程安全的
    //synchronized意味获取pageMap归我所有，获取pageMap锁
    public boolean gainSharedLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        if (pageMap.get(pid) == null) {
            HashSet<TransactionId> readers = new HashSet<>();
            readers.add(tid);
            pageMap.put(pid, new SharedLock(readers));
            return true;
        } else if (pageMap.get(pid) instanceof SharedLock) {
            SharedLock lock = (SharedLock) pageMap.get(pid);
            lock.readers.add(tid);
            return true;
        } else {
            ExclusiveLock lock = (ExclusiveLock) pageMap.get(pid);
            if (lock.owner.equals(tid))
                return true;
        }
        return false;
    }

    public boolean gainExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        if (pageMap.get(pid) == null) {
            pageMap.put(pid, new ExclusiveLock(tid));
            return true;
        } else if (pageMap.get(pid) instanceof SharedLock) {
            SharedLock lock = (SharedLock) pageMap.get(pid);
            if (lock.readers.size() == 1 && lock.readers.contains(tid)) {
                pageMap.remove(pid);
                pageMap.put(pid, new ExclusiveLock(tid));
                return true;
            }
        } else {
            ExclusiveLock lock = (ExclusiveLock) pageMap.get(pid);
            if (lock.owner.equals(tid))
                return true;
        }
        return false;
    }

    public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if (perm.equals(Permissions.READ_ONLY)) return gainSharedLock(tid, pid);
        else return gainExclusiveLock(tid, pid);
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        if (!pageMap.containsKey(pid)) return;
        Lock lock = pageMap.get(pid);
        if (lock instanceof SharedLock) {
            SharedLock slock = (SharedLock) lock;
            slock.readers.remove(tid);
            if (slock.readers.isEmpty()) pageMap.remove(pid);
        } else {
            ExclusiveLock elock = (ExclusiveLock) lock;
            if (elock.owner.equals(tid)) {
                pageMap.remove(pid);
            }
        }
    }

    public List<PageId> getPages(TransactionId tid) {
        List<PageId> res = new ArrayList<>();
        for (Map.Entry<PageId, Lock> entry : pageMap.entrySet()) {
            Lock lock = entry.getValue();
            if (lock instanceof SharedLock) {
                if (((SharedLock) lock).readers.contains(tid)) res.add(entry.getKey());
            } else {
                if (((ExclusiveLock) lock).owner.equals(tid)) res.add(entry.getKey());
            }
        }
        return res;
    }
}
