package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private int buckets;
    private int min;
    private int max;
    private double width;
    private int ntups = 0;

    private int[] histogram;

    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = (max-min+1 < buckets) ? (max-min+1) : buckets;
        this.min = min;
        this.max = max;
        width = (double) (max - min) / (double) this.buckets;
        histogram = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index = (int) Math.floor(((double) (v - min) / (double) (max - min)) * (buckets - 1));
        histogram[index]++;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int index = (int) Math.floor((double) (v - min) / (double) (max - min) * (buckets - 1));
        int bleft = min + (int) (index * width);
        int bright = min + (int) ((index + 1) * width);
        double leftPercentage = 0.00;
        double rightPercentage = 0.00;
        for (int i = 0; i < buckets; i++) {
            if (i < index) leftPercentage += histogram[i] / (double) ntups;
            if (i > index) rightPercentage += histogram[i] / (double) ntups;
        }
        switch (op) {
            case LIKE:
            case EQUALS:
                if (!inside(v)) return 0;
                return histogram[index] / width / (double) ntups;
            case NOT_EQUALS:
                if (!inside(v)) return 1;
                return 1 - histogram[index] / width / (double) ntups;
            case GREATER_THAN:
                if (v < min) return 1;
                if (v > max) return 0;
                return (histogram[index] / (double) ntups) * ((bright - v) / width) + rightPercentage;
            case LESS_THAN:
                if (v < min) return 0;
                if (v > max) return 1;
                return (histogram[index] / (double) ntups) * ((v - bleft) / width) + leftPercentage;
            case GREATER_THAN_OR_EQ:
                if (v < min) return 1;
                if (v > max) return 0;
                return (histogram[index] / (double) ntups) * ((bright - v) / width) + rightPercentage + histogram[index] / width / (double) ntups;
            case LESS_THAN_OR_EQ:
                if (v < min) return 0;
                if (v > max) return 1;
                return (histogram[index] / (double) ntups) * ((v - bleft) / width) + leftPercentage + histogram[index] / width / (double) ntups;
        }
        return -1.0;
    }

    public boolean inside(int v) {
        return v >= min && v <= max;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String s = "";
        for (int i = 0; i < buckets; i++) {
            s += i + ": " + histogram[i];
            if (i != buckets) s += ", ";
        }
        return "ntups: " + ntups + " buckets: " + buckets + " width: " + width + " max:" + max + " min:" + min + " histogram: {" + s + " }";
    }
}
