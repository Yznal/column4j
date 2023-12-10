package org.column4j.column.statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Int8Statistic extends Statistic {


    /**
     * Called each time when a value in chunk  has changed.
     *
     * @param position changed position in chunk
     * @param oldValue old value
     * @param newValue new value
     */
    void onValueChanged(int position, byte oldValue, byte newValue);

    /**
     * Called each time when a tombstone value in chunk has filled.
     *
     * @param position changed position in chunk
     * @param newValue new value
     */
    void onValueAdded(int position, byte newValue);

    /**
     * Called each time when a value in chunk has changed to tombstone.
     *
     * @param position changed position in chunk
     * @param oldValue old value
     */
    void onValueRemoved(int position, byte oldValue);

    /**
     * @return first value in chunk
     */
    byte getFirstValue();

    /**
     * @return last value in chunk
     */
    byte getLastValue();

    /**
     * @return min value in chunk
     */
    byte getMin();

    /**
     * @return max value in chunk
     */
    byte getMax();

    /**
     * @return sum of all values in chunk
     */
    long getSum();
}
