package org.column4j.index.v2;


import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public interface IndexColumn<ResultType> {

    // TODO change ret type to block links? make global block storage and return indices?
    @Nullable
    ResultType lookupIndex(CharSequence key);

    void store(CharSequence key, int value);

    int cardinality();

}
