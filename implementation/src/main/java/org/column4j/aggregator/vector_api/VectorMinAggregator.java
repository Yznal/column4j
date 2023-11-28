package org.column4j.aggregator.vector_api;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.column4j.ColumnVector;
import org.column4j.aggregator.IntAggregator;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class VectorMinAggregator implements IntAggregator {

    public final VectorSpecies<Integer> speciesPreferred;
    public final int speciesLength;

    public VectorMinAggregator() {
        this.speciesPreferred = IntVector.SPECIES_PREFERRED;
        this.speciesLength = speciesPreferred.length();
    }

    @Override
    public int aggregate(ColumnVector<int[]> column, int from, int to) {
        var data = column.getData();
        from = Math.max(from, column.firstRowIndex());
        var intVector = IntVector.broadcast(speciesPreferred, Integer.MAX_VALUE);
        for(; from < to && from + speciesLength <= to; from += speciesLength) {
            var nextIntVector = IntVector.fromArray(speciesPreferred, data, from);
            intVector = intVector.min(nextIntVector);
        }
        var min = intVector.reduceLanes(VectorOperators.MIN);
        // tail
        for(; from < to; from++) {
            min = Math.min(min, data[from]);
        }
        return min;
    }
}
