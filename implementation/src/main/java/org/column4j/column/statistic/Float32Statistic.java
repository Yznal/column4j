package org.column4j.column.statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Float32Statistic extends Statistic {


    /**
     * Called each time when a value in chunk  has changed.
     *
     * @param position changed position in chunk
     * @param oldValue old value
     * @param newValue new value
     */
    void onValueChanged(int position, float oldValue, float newValue);

    /**
     * Called each time when a tombstone value in chunk has filled.
     *
     * @param position changed position in chunk
     * @param newValue new value
     */
    void onValueAdded(int position, float newValue);

    /**
     * Called each time when a value in chunk has changed to tombstone.
     *
     * @param position changed position in chunk
     * @param oldValue old value
     */
    void onValueRemoved(int position, float oldValue);

    /**
     * @return first value in chunk
     */
    float getFirstValue();

    /**
     * @return last value in chunk
     */
    float getLastValue();

    /**
     * @return min value in chunk
     */
    float getMin();

    /**
     * @return max value in chunk
     */
    float getMax();

    /**
     * @return sum of all values in chunk
     */
    double getSum();
}
