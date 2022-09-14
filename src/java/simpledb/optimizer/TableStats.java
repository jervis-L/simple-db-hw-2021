package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
//          set方法原本是不可见的
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid
     * The table over which to compute statistics
     * @param ioCostPerPage
     * The cost per page of IO. This doesn't differentiate between
     * sequential-scan IO and disk seeks.
     */
    private int tableid;
    private final TransactionId td;
    private DbFile file;
    private int ioCostPerPage;
    private int ntups = 0;
    private TupleDesc tupleDesc;
    private int[] min;
    private int[] max;
    private List<Object> histograms=new ArrayList<>();


    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        td = new TransactionId();
        file = Database.getCatalog().getDatabaseFile(tableid);
        DbFileIterator it = file.iterator(td);
        tupleDesc=file.getTupleDesc();
        int numfields = tupleDesc.numFields();
        min=new int[numfields];
        max=new int[numfields];
        try {
            it.open();
            while (it.hasNext()) {
                Tuple t = it.next();
                Iterator<Field> fields = t.fields();
                int index=0;
                while (fields.hasNext()) {
                    Field next = fields.next();
                    if(tupleDesc.getFieldType(index)==Type.INT_TYPE){
                        IntField f= (IntField) next;
                        min[index]=Math.min(min[index],f.getValue());
                        max[index]=Math.max(max[index],f.getValue());
                    }
                    index++;
                }
                ntups++;
            }
            it.rewind();
            for(int i=0;i<numfields;i++){
                if(tupleDesc.getFieldType(i)==Type.INT_TYPE){
                    histograms.add(new IntHistogram(NUM_HIST_BINS,min[i],max[i]));
                }else{
                    histograms.add(new StringHistogram(NUM_HIST_BINS));
                }
            }
            while (it.hasNext()) {
                Tuple t = it.next();
                Iterator<Field> fields = t.fields();
                int index=0;
                while (fields.hasNext()) {
                    Field next = fields.next();
                    if(tupleDesc.getFieldType(index)==Type.INT_TYPE){
                        IntField f= (IntField) next;
                        IntHistogram hist= (IntHistogram) histograms.get(index);
                        hist.addValue(f.getValue());
                    }else{
                        StringField f= (StringField) next;
                        StringHistogram hist= (StringHistogram) histograms.get(index);
                        hist.addValue(f.getValue());
                    }
                    index++;
                }
            }
            it.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return ioCostPerPage * ((HeapFile)file).numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)Math.round(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Object o=histograms.get(field);
        if(tupleDesc.getFieldType(field)==Type.INT_TYPE){
            IntHistogram hist= (IntHistogram) o;
            System.out.println(hist.toString());
            IntField f= (IntField) constant;
            return hist.estimateSelectivity(op,f.getValue());
        }else{
            StringHistogram hist= (StringHistogram) o;
            StringField f= (StringField) constant;
            return hist.estimateSelectivity(op,f.getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return ntups;
    }

}
