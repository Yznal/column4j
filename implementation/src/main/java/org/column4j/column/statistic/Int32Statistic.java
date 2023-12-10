package org.column4j.column.statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Int32Statistic extends Statistic {

    /**
     * Called each time when a value in chunk  has changed.
     *
     * @param position changed position in chunk
     * @param oldValue old value
     * @param newValue new value
     */
    void onValueChanged(int position, int oldValue, int newValue);

    /**
     * Called each time when a tombstone value in chunk has filled.
     *
     * @param position changed position in chunk
     * @param newValue new value
     */
    void onValueAdded(int position, int newValue);

    /**
     * Called each time when a value in chunk has changed to tombstone.
     *
     * @param position changed position in chunk
     * @param oldValue old value
     */
    void onValueRemoved(int position, int oldValue);

    /**
     * @return first value in chunk
     */
    int getFirstValue();

    /**
     * @return last value in chunk
     */
    int getLastValue();

    /**
     * @return min value in chunk
     */
    int getMin();

    /**
     * @return max value in chunk
     */
    int getMax();

    /**
     * @return sum of all values in chunk
     */
    long getSum();
}
