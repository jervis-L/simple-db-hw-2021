package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import sun.misc.JavaNioAccess;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     * the file that stores the on-disk backing store for this heap
     * file.
     */
    private File f;
    private TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }


    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
//        long offset = pid.getPageNumber() * BufferPool.getPageSize();
//        byte[] data = new byte[BufferPool.getPageSize()];
//        try {
//            RandomAccessFile rFile = new RandomAccessFile(f, "r");
//            rFile.seek(offset);
//            for (int i = 0; i < BufferPool.getPageSize(); i++) {
//                data[i] = (byte) rFile.read();
//            }
//            HeapPage page = new HeapPage((HeapPageId) pid, data);
//            rFile.close();
//            return page;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            int pagesize = BufferPool.getPageSize();
            byte[] b = new byte[pagesize];
            raf.seek(pid.getPageNumber() * pagesize);
            raf.read(b, 0, pagesize);
            raf.close();
            return new HeapPage((HeapPageId)pid, b);
        } catch(Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        long offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        RandomAccessFile raf=new RandomAccessFile(f, "rw");
        raf.seek(offset);
        raf.write(page.getPageData(),0,BufferPool.getPageSize());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(f.length()/ BufferPool.getPageSize());
    }

//    // see DbFile.java for javadocs
//    public List<Page> insertTuple(TransactionId tid, Tuple t)
//            throws DbException, IOException, TransactionAbortedException {
//        // some code goes here
//        for(int i=0;i<numPages();i++){
//            PageId pid=new HeapPageId(getId(),i);
//            HeapPage page= (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
//            if(page.getNumEmptySlots()!=0){
//                Database.getLockManager().gainExclusiveLock(tid,pid);
//                page.insertTuple(t);
//                return new ArrayList<>(Arrays.asList(page));
//            }
//            Database.getLockManager().releaseLock(tid,pid);
//        }
//        HeapPageId pid=new HeapPageId(getId(),numPages());
//        HeapPage page=new HeapPage(pid,HeapPage.createEmptyPageData());
//        page.insertTuple(t);
//        synchronized (f){
//            writePage(page);
//        }
//        return new ArrayList<>(Arrays.asList(page));
//        // not necessary for lab1
//    }
//
//    // see DbFile.java for javadocs
//    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
//            TransactionAbortedException {
//        // some code goes here
//        PageId pid=t.getRecordId().getPageId();
//        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
//        page.deleteTuple(t);
//        return new ArrayList<>(Arrays.asList(page));
//        // not necessary for lab1
//    }
// see DbFile.java for javadocs
public List<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    // some code goes here
    HeapPage page  = null;

    // find a non full page
    for(int i=0;i<numPages();++i){
        HeapPageId pid = new HeapPageId(getId(),i);
        page = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        if(page.getNumEmptySlots()!=0){
            break;
        }
        else{
            Database.getBufferPool().unsafeReleasePage(tid,pid);
        }
    }

    // if not exist an empty slot, create a new page to store
    if(page == null || page.getNumEmptySlots() == 0){
        HeapPageId pid = new HeapPageId(getId(),numPages());
        byte[] data = HeapPage.createEmptyPageData();
        HeapPage heapPage = new HeapPage(pid,data);
        writePage(heapPage);
        page = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
    }

    page.insertTuple(t);

    List<Page> res = new ArrayList<>();
    res.add(page);
    return res;
}


    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();

        // delete tuple and mark page as dirty
        HeapPage page =  (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        page.deleteTuple(t);

        // return res
        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(final TransactionId tid){
        DbFileIterator dbFileIterator=new DbfileIteratorImpl(tid);
        return dbFileIterator;
    }
    private class DbfileIteratorImpl implements DbFileIterator {
        TransactionId tid;
        int curPageNo=0;
        Iterator<Tuple> curPageIterator;
        public DbfileIteratorImpl(final TransactionId tid) {
            this.tid = tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            refreshIterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(curPageIterator==null || numPages()<0) return false;
            if(curPageNo<numPages()){
                while(!curPageIterator.hasNext()&&curPageNo<numPages()-1){
                    curPageNo++;
                    refreshIterator();
                }
                return curPageIterator.hasNext();
            }
            return false;
        }
        public void refreshIterator() throws TransactionAbortedException, DbException {
            PageId pid=new HeapPageId(getId(),curPageNo);
            HeapPage page= (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
            curPageIterator=page.iterator();
        }
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext()) throw new NoSuchElementException();
            if(curPageIterator.hasNext()) {
                return curPageIterator.next();
            }
            throw new NoSuchElementException();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            curPageNo=0;
            refreshIterator();
        }

        @Override
        public void close() {
            curPageIterator=null;
        }
    }
}

