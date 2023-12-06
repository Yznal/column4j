package org.column4j.table.query.select.criteria;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface CriteriaQueryStep<P> {

    /**
     * Add criteria "cell value equals to" in query
     *
     * @param name  column name
     * @param value value to filter
     * @return self reference
     */
    P eq(String name, byte value);

    /**
     * Add criteria "cell value equals to" in query
     *
     * @param name  column name
     * @param value value to filter
     * @return self reference
     */
    P eq(String name, short value);

    /**
     * Add criteria "cell value equals to" in query
     *
     * @param name  column name
     * @param value value to filter
     * @return self reference
     */
    P eq(String name, int value);

    /**
     * Add criteria "cell value equals to" in query
     *
     * @param name  column name
     * @param value value to filter
     * @return self reference
     */
    P eq(String name, long value);

    /**
     * Add criteria "cell value equals to" in query
     *
     * @param name  column name
     * @param value value to filter
     * @return self reference
     */
    P eq(String name, float value);

    /**
     * Add criteria "cell value equals to" in query
     *
     * @param name  column name
     * @param value value to filter
     * @return self reference
     */
    P eq(String name, double value);

    /**
     * Add criteria "cell value equals to" in query
     *
     * @param name  column name
     * @param value value to filter
     * @return self reference
     */
    P eq(String name, String value);

    /**
     * Add criteria "cell value contains" in query
     *
     * @param name  column name
     * @param value value to filter
     * @return self reference
     */
    P contains(String name, String value);

    /**
     * Add criteria "cell value in bound" in query
     *
     * @param name          column name
     * @param from          left bound
     * @param inclusiveFrom left bound is inclusive
     * @param to            right bound
     * @param inclusiveTo   right bound is inclusive
     * @return self reference
     */
    P between(String name, byte from, boolean inclusiveFrom, byte to, boolean inclusiveTo);

    /**
     * Add criteria "cell value in bound" in query
     *
     * @param name          column name
     * @param from          left bound
     * @param inclusiveFrom left bound is inclusive
     * @param to            right bound
     * @param inclusiveTo   right bound is inclusive
     * @return self reference
     */
    P between(String name, short from, boolean inclusiveFrom, short to, boolean inclusiveTo);

    /**
     * Add criteria "cell value in bound" in query
     *
     * @param name          column name
     * @param from          left bound
     * @param inclusiveFrom left bound is inclusive
     * @param to            right bound
     * @param inclusiveTo   right bound is inclusive
     * @return self reference
     */
    P between(String name, int from, boolean inclusiveFrom, int to, boolean inclusiveTo);

    /**
     * Add criteria "cell value in bound" in query
     *
     * @param name          column name
     * @param from          left bound
     * @param inclusiveFrom left bound is inclusive
     * @param to            right bound
     * @param inclusiveTo   right bound is inclusive
     * @return self reference
     */
    P between(String name, long from, boolean inclusiveFrom, long to, boolean inclusiveTo);

    /**
     * Add criteria "cell value in bound" in query
     *
     * @param name          column name
     * @param from          left bound
     * @param inclusiveFrom left bound is inclusive
     * @param to            right bound
     * @param inclusiveTo   right bound is inclusive
     * @return self reference
     */
    P between(String name, float from, boolean inclusiveFrom, float to, boolean inclusiveTo);

    /**
     * Add criteria "cell value in bound" in query
     *
     * @param name          column name
     * @param from          left bound
     * @param inclusiveFrom left bound is inclusive
     * @param to            right bound
     * @param inclusiveTo   right bound is inclusive
     * @return self reference
     */
    P between(String name, double from, boolean inclusiveFrom, double to, boolean inclusiveTo);

}
