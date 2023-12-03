package org.column4j.table;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Table {

    /**
     * Factory method to create TableBuilder
     * @return instance of TableBuilder
     */
    static TableBuilder builder() {
        return new TableImpl.TableBuilderImpl();
    }

    /**
     * Start insertion query
     */
    InsertQuery insert();

    /**
     * What kind of select request we have to implement?
     */

}
