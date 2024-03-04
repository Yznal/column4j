package org.column4j.column.statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface StringStatistic extends Statistic {

    /**
     * Called each time when a value in chunk  has changed.
     *
     * @param position changed position in chunk
     * @param oldValue old value
     * @param newValue new value
     */
    void onValueChanged(int position, String oldValue, String newValue);

    /**
     * Called each time when a tombstone value in chunk has filled.
     *
     * @param position changed position in chunk
     * @param newValue new value
     */
    void onValueAdded(int position, String newValue);

    /**
     * Called each time when a value in chunk has changed to tombstone.
     *
     * @param position changed position in chunk
     * @param oldValue old value
     */
    void onValueRemoved(int position, String oldValue);

    /**
     * @return first value in chunk
     */
    String getFirstValue();

    /**
     * @return last value in chunk
     */
    String getLastValue();
}
