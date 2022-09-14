package simpledb.storage;

/**
 * @author jervisliao
 * @create 2022-08-26 20:10
 */
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;

import simpledb.transaction.LockManager;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPoolJervis {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

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
    private final int numPages;

    public BufferPoolJervis(int numPages) {
        this.numPages = numPages;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPoolJervis.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPoolJervis.pageSize = DEFAULT_PAGE_SIZE;
    }


    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    ConcurrentHashMap<PageId, Page> map = new ConcurrentHashMap<>();
    Deque<Page> deque = new ArrayDeque<>();

    private static final long MAX_TIMEOUT = 2000;

    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        long timeout= (long)(Math.random() * MAX_TIMEOUT);
        long start=System.currentTimeMillis();
        boolean isLocked=false;
        while(!isLocked){
            if(System.currentTimeMillis()-start>timeout)throw new TransactionAbortedException();
            isLocked=Database.getLockManager().acquireLock(tid,pid,perm);
        }
        if (map.containsKey(pid)) {
            return map.get(pid);
        }
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = file.readPage(pid);
        if (page != null) {
            insertPage(page);
            return page;
        }
        throw new DbException("Invalid PageId...");
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
        // some code goes here
        // not necessary for lab1|lab2
//        releaseLock(tid, pid);
        Database.getLockManager().releaseLock(tid,pid);
    }


    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        return Database.getLockManager().holdsLock(tid,pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit){
        // some code goes here
        // not necessary for lab1|lab2
        try {
            if(commit){
                flushPages(tid);
            }else{
                for(PageId pid:Database.getLockManager().getPages(tid)){
                    DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                    Page page = file.readPage(pid);
                    if (page != null) {
                        insertPage(page);
                    }
                    Database.getLockManager().releaseLock(tid,pid);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = file.insertTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
            insertPage(page);
        }
    }

    public void insertPage(Page page) throws DbException {
        if (!map.containsKey(page.getId())) {
            if (map.size() < numPages) {
                map.put(page.getId(), page);
                deque.offer(page);
            } else {
                evictPage();
                map.put(page.getId(), page);
                deque.offer(page);
            }
        } else {
            map.put(page.getId(), page);
            deque.remove(page);
            deque.offer(page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.recordId.getPageId().getTableId());
        List<Page> pages = file.deleteTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
            insertPage(page);
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

        for (PageId pid : map.keySet()) {
            flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        deque.remove(map.get(pid));
        map.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page p = map.get(pid);
        if (p == null || p.isDirty()==null) return;
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        f.writePage(p);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(PageId pid:Database.getLockManager().getPages(tid)){
            flushPage(pid);
            Database.getLockManager().releaseLock(tid,pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Iterator<Page> it =deque.iterator();
        while(it.hasNext()){
            Page page=it.next();
            if(page.isDirty()==null){
                discardPage(page.getId());
                return;
            }
        }
        throw new DbException("too many dirty pages");
    }
}
