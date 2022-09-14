package simpledb.common;

import simpledb.storage.BufferPool;
import simpledb.storage.LogFile;
import simpledb.transaction.LockManager;

import java.io.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Database is a class that initializes several static variables used by the
 * database system (the catalog, the buffer pool, and the log files, in
 * particular.)
 * <p>
 * Provides a set of methods that can be used to access these variables from
 * anywhere.
 * 
 * @Threadsafe
 */
public class Database {
    //以原子方式更新对象引用
    //可以看出这是单例模式，static final变量保留着唯一一个Database实例，并不允许修改
    //不过这个饿汉式单例不允许访问自身，只是提供访问Catalog/BufferPool的方式
    private static final AtomicReference<Database> _instance = new AtomicReference<>(new Database());
    private final Catalog _catalog;
    private final BufferPool _bufferpool;
    private final LockManager _lockmanager;

    private final static String LOGFILENAME = "log";
    private final LogFile _logfile;

    private Database() {
        _catalog = new Catalog();
        _bufferpool = new BufferPool(BufferPool.DEFAULT_PAGES);
        _lockmanager=new LockManager();
        LogFile tmp = null;
        try {
            tmp = new LogFile(new File(LOGFILENAME));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        _logfile = tmp;
        // startControllerThread();
    }

    /** Return the log file of the static Database instance */
    public static LogFile getLogFile() {
        return _instance.get()._logfile;
    }

    /** Return the buffer pool of the static Database instance */
    public static BufferPool getBufferPool() {
        return _instance.get()._bufferpool;
    }

    /** Return the catalog of the static Database instance */
    public static Catalog getCatalog() {
        return _instance.get()._catalog;
    }
    /** Return the catalog of the static Database instance */

    public static LockManager getLockManager(){return _instance.get()._lockmanager;}

    /**
     * Method used for testing -- create a new instance of the buffer pool and
     * return it
     */
    public static BufferPool resetBufferPool(int pages) {
        java.lang.reflect.Field bufferPoolF=null;
        java.lang.reflect.Field LockManagerF=null;
        try {
            bufferPoolF = Database.class.getDeclaredField("_bufferpool");
            bufferPoolF.setAccessible(true);
            bufferPoolF.set(_instance.get(), new BufferPool(pages));
            LockManagerF=Database.class.getDeclaredField("_lockmanager");
            LockManagerF.setAccessible(true);
            LockManagerF.set(_instance.get(),new LockManager());
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }
//        _instance._bufferpool = new BufferPool(pages);
        return _instance.get()._bufferpool;
    }

    // reset the database, used for unit tests only.
    public static void reset() {
        _instance.set(new Database());
    }

}
